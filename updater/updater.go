package updater

import (
	"io/ioutil"
	"math"
	"net/http"
	"regexp"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/spf13/viper"

	"github.com/wtg/shuttletracker"
	"github.com/wtg/shuttletracker/log"
)

// DataFeedResponse contains information from the iTRAK data feed.
type DataFeedResponse struct {
	Body       []byte
	StatusCode int
	Headers    http.Header
}

// Updater handles periodically grabbing the latest vehicle location data from iTrak.
type Updater struct {
	cfg                  Config
	updateInterval       time.Duration
	dataRegexp           *regexp.Regexp
	ms                   shuttletracker.ModelService
	mutex                *sync.Mutex
	lastDataFeedResponse *DataFeedResponse
}

type Config struct {
	DataFeed       string
	UpdateInterval string
}

// New creates an Updater.
func New(cfg Config, ms shuttletracker.ModelService) (*Updater, error) {
	// Create Updater object
	updater := &Updater{
		cfg:   cfg,
		ms:    ms,
		mutex: &sync.Mutex{},
	}

	// err gets filled and returns "nil" if ParseDuration returns an error
	interval, err := time.ParseDuration(cfg.UpdateInterval)
	if err != nil {
		return nil, err
	}
	updater.updateInterval = interval

	// Match each API field with any number (+)
	//   of the previous expressions (\d digit, \. escaped period, - negative number)
	//   Specify named capturing groups to store each field from data feed
	updater.dataRegexp = regexp.MustCompile(`(?P<id>Vehicle ID:([\d\.]+)) (?P<lat>lat:([\d\.-]+)) (?P<lng>lon:([\d\.-]+)) (?P<heading>dir:([\d\.-]+)) (?P<speed>spd:([\d\.-]+)) (?P<lock>lck:([\d\.-]+)) (?P<time>time:([\d]+)) (?P<date>date:([\d]+)) (?P<status>trig:([\d]+))`)

	return updater, nil
}

func NewConfig(v *viper.Viper) *Config {
	// Create Config object
	cfg := &Config{
		UpdateInterval: "10s",
		DataFeed:       "https://shuttles.rpi.edu/datafeed",
	}
	v.SetDefault("updater.updateinterval", cfg.UpdateInterval)
	v.SetDefault("updater.datafeed", cfg.DataFeed)
	return cfg
}

// Run updater forever.
func (u *Updater) Run() {
	log.Debug("Updater started.")
	ticker := time.Tick(u.updateInterval)

	// Do one initial update.
	u.update()

	// Call update() every updateInterval.
	for range ticker {
		u.update()
	}
}

// Send a request to iTrak API, get updated shuttle info,
// store updated records in the database, and remove old records.
func (u *Updater) update() {
	// Make request to iTrak data feed
	client := http.Client{Timeout: time.Second * 5}
	// HTTP GET request from https://shuttles.rpi.edu/datafeed
	resp, err := client.Get(u.cfg.DataFeed)
	if err != nil {
		log.WithError(err).Error("Could not get data feed.")
		return
	}

	// Prints errors in the case that the GET request doesn't give StatusOK
	if resp.StatusCode != http.StatusOK {
		log.Errorf("data feed status code %d", resp.StatusCode)
		return
	}

	// Read response body content
	body, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		log.WithError(err).Error("Could not read data feed.")
		return
	}
	resp.Body.Close()

	// Create a DataFeedResponseobject in dfresp
	dfresp := &DataFeedResponse{
		Body:       body,
		StatusCode: resp.StatusCode,
		Headers:    resp.Header,
	}
	// Sets the variable lastDataFeedResponse to dfresp in a protected manner
	u.setLastResponse(dfresp)

	delim := "eof"
	// split the body of response by delimiter
	vehiclesData := strings.Split(string(body), delim)
	vehiclesData = vehiclesData[:len(vehiclesData)-1] // last element is EOF

	// TODO: Figure out if this handles == 1 vehicle correctly or always assumes > 1.
	if len(vehiclesData) <= 1 {
		log.Warnf("Found no vehicles delineated by '%s'.", delim)
	}

	wg := sync.WaitGroup{}
	// for parsed data, update each vehicle
	for _, vehicleData := range vehiclesData {
		wg.Add(1)
		go func(vehicleData string) {
			u.handleVehicleData(vehicleData)
			wg.Done()
		}(vehicleData)
	}
	wg.Wait()
	log.Debugf("Updated vehicles.")

	// Prune updates older than one month
	deleted, err := u.ms.DeleteLocationsBefore(time.Now().AddDate(0, -1, 0))
	if err != nil {
		log.WithError(err).Error("unable to remove old locations")
		return
	}
	if deleted > 0 {
		log.Debugf("Removed %d old updates.", deleted)
	}
}

// nolint: gocyclo
func (u *Updater) handleVehicleData(vehicleData string) {
	match := u.dataRegexp.FindAllStringSubmatch(vehicleData, -1)[0]
	// Store named capturing group and matching expression as a key value pair
	result := map[string]string{}
	for i, item := range match {
		result[u.dataRegexp.SubexpNames()[i]] = item
	}

	// Create new vehicle update & insert update into database

	// Parses out the string "Vehicle ID:" so itrakID is just the ID itself
	itrakID := strings.Replace(result["id"], "Vehicle ID:", "", -1)
	vehicle, err := u.ms.VehicleWithTrackerID(itrakID)
	// Handles error checking in the case vehicles are unknown
	if err == shuttletracker.ErrVehicleNotFound {
		log.Warnf("Unknown vehicle ID \"%s\" returned by iTrak. Make sure all vehicles have been added.", itrakID)
		return
	} else if err != nil {
		log.WithError(err).Error("Unable to fetch vehicle.")
		return
	}

	// determine if this is a new update from itrak by comparing timestamps
	newTime, err := itrakTimeDate(result["time"], result["date"])
	if err != nil {
		log.WithError(err).Error("unable to parse iTRAK time and date")
		return
	}

	lastUpdate, err := u.ms.LatestLocation(vehicle.ID)
	if err != nil && err != shuttletracker.ErrLocationNotFound {
		log.WithError(err).Error("unable to retrieve last update")
		return
	}
	if err != shuttletracker.ErrLocationNotFound && newTime.Equal(lastUpdate.Time) {
		// Timestamp is not new; don't store update.
		return
	}
	log.Debugf("Updating %s.", vehicle.Name)

	// vehicle found and no error
	route, err := u.GuessRouteForVehicle(vehicle)
	if err != nil {
		log.WithError(err).Error("Unable to guess route for vehicle.")
		return
	}

	// Sets latitude and longitude by removing the strings "lat:", "lon" and
	// "dir" from the numbers themselves
	latitude, err := strconv.ParseFloat(strings.Replace(result["lat"], "lat:", "", -1), 64)
	if err != nil {
		log.WithError(err).Error("unable to parse latitude as float")
		return
	}
	longitude, err := strconv.ParseFloat(strings.Replace(result["lng"], "lon:", "", -1), 64)
	if err != nil {
		log.WithError(err).Error("unable to parse longitude as float")
		return
	}
	heading, err := strconv.ParseFloat(strings.Replace(result["heading"], "dir:", "", -1), 64)
	if err != nil {
		log.WithError(err).Error("unable to parse heading as float")
		return
	}
	// convert KPH to MPH
	speedKMH, err := strconv.ParseFloat(strings.Replace(result["speed"], "spd:", "", -1), 64)
	if err != nil {
		log.Error(err)
		return
	}
	speedMPH := kphToMPH(speedKMH)

	trackerID := strings.Replace(result["id"], "Vehicle ID:", "", -1)

	// Create a new shuttletracker.Location object in update
	update := &shuttletracker.Location{
		TrackerID: trackerID,
		Latitude:  latitude,
		Longitude: longitude,
		Heading:   heading,
		Speed:     speedMPH,
		Time:      newTime,
	}
	if route != nil {
		update.RouteID = &route.ID
	}

	// Creates the location if err isn't nil: in line command
	if err := u.ms.CreateLocation(update); err != nil {
		log.WithError(err).Errorf("could not create location")
	}
}

// Convert kmh to mph
func kphToMPH(kmh float64) float64 {
	return kmh * 0.621371192
}

// GuessRouteForVehicle returns a guess at what route the vehicle is on.
// It may return an empty route if it does not believe a vehicle is on any route.
// nolint: gocyclo
func (u *Updater) GuessRouteForVehicle(vehicle *shuttletracker.Vehicle) (route *shuttletracker.Route, err error) {
	// ms.Routes() just grabs the routes in a pointer array format
	routes, err := u.ms.Routes()
	if err != nil {
		return nil, err
	}

	// Create new dynamic array routeDistances, and the loop initializes all to 0
	// routeDistances will hold the route distances, and the min is the approximate
	routeDistances := make(map[int64]float64)
	for _, route := range routes {
		routeDistances[route.ID] = 0
	}

	updates, err := u.ms.LocationsSince(vehicle.ID, time.Now().Add(time.Minute*-15))
	if len(updates) < 5 {
		// Can't make a guess with fewer than 5 updates.
		log.Debugf("%v has too few recent updates (%d) to guess route.", vehicle.Name, len(updates))
		return
	}

	// Uses updates to approximate route
	for _, update := range updates {
		for _, route := range routes {
			if !route.Enabled || !route.Active {
				routeDistances[route.ID] += math.Inf(0)
			}
			nearestDistance := math.Inf(0)
			// Calculate distance with basic distance formula, and update nearest
			for _, point := range route.Points {
				distance := math.Sqrt(math.Pow(update.Latitude-point.Latitude, 2) +
					math.Pow(update.Longitude-point.Longitude, 2))
				if distance < nearestDistance {
					nearestDistance = distance
				}
			}
			if nearestDistance > .003 {
				nearestDistance += 50
			}
			// Append to routeDistances
			routeDistances[route.ID] += nearestDistance
		}
	}

	// Goes through the routeDistances list and finds the smallest one, which is
	// the approximate.
	minDistance := math.Inf(0)
	var minRouteID int64
	for id := range routeDistances {
		distance := routeDistances[id] / float64(len(updates))
		if distance < minDistance {
			minDistance = distance
			minRouteID = id
			// If more than ~5% of the last 100 samples were far away from a route, say the shuttle is not on a route
			// This is extremely aggressive and requires a shuttle to be on a route for ~5 minutes before it registers as on the route
			if minDistance > 5 {
				minRouteID = 0
			}
		}
	}

	// not on a route
	if minRouteID == 0 {
		log.Debugf("%v not on route; distance from nearest: %v", vehicle.Name, minDistance)
		return nil, nil
	}

	// Create "route" as the guess and return it
	route, err = u.ms.Route(minRouteID)
	if err != nil {
		return route, err
	}
	log.Debugf("%v on %s route.", vehicle.Name, route.Name)
	return route, err
}

func itrakTimeDate(itrakTime, itrakDate string) (time.Time, error) {
	// Add leading zeros to the time value if they're missing. time.Parse expects this.
	if len(itrakTime) < 11 {
		builder := itrakTime[:5]
		for i := len(itrakTime); i < 11; i++ {
			builder += "0"
		}
		builder += itrakTime[5:]
		itrakTime = builder
	}

	combined := itrakDate + " " + itrakTime
	return time.Parse("date:01022006 time:150405", combined)
}

// Locks and unlocks the mutex in order to avoid errors in synchronization
func (u *Updater) setLastResponse(dfresp *DataFeedResponse) {
	u.mutex.Lock()
	u.lastDataFeedResponse = dfresp
	u.mutex.Unlock()
}

// GetLastResponse returns the most recent response from the iTRAK data feed.
func (u *Updater) GetLastResponse() *DataFeedResponse {
	u.mutex.Lock()
	defer u.mutex.Unlock()
	return u.lastDataFeedResponse
}
