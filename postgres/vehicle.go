package postgres

import (
	"database/sql"

	// Postgres driver for database/sql
	_ "github.com/lib/pq"

	"github.com/wtg/shuttletracker"
)

// VehicleService implements shuttletracker.VehicleService.
type VehicleService struct {
	db *sql.DB
}

// Initializes how the data is represented in the Postgres database
func (v *VehicleService) initializeSchema(db *sql.DB) error {
	// Postgres command that cretes a vehicle table in the database
	v.db = db
	schema := `
-- DROP TABLE vehicles;
CREATE TABLE IF NOT EXISTS vehicles (
    id serial PRIMARY KEY,
	name text,
	created timestamp with time zone NOT NULL DEFAULT now(),
	updated timestamp with time zone NOT NULL DEFAULT now(),
	enabled boolean NOT NULL,
	tracker_id varchar(10) UNIQUE
);
    `
	_, err := v.db.Exec(schema)
	return err
}

// CreateVehicle creates a Vehicle.
func (v *VehicleService) CreateVehicle(vehicle *shuttletracker.Vehicle) error {
	// Postgres command that cretes a vehicle in the database
	statement := "INSERT INTO vehicles (name, enabled, tracker_id) " +
		"VALUES ($1, $2, $3) RETURNING id, created, updated;"
	row := v.db.QueryRow(statement, vehicle.Name, vehicle.Enabled, vehicle.TrackerID)
	// If this function is successful, it should return "nil"
	err := row.Scan(&vehicle.ID, &vehicle.Created, &vehicle.Updated)
	return err
}

// DeleteVehicle deletes a Vehicle by its ID.
func (v *VehicleService) DeleteVehicle(id int64) error {
	statement := "DELETE FROM vehicles WHERE id = $1;"
	result, err := v.db.Exec(statement, id)
	if err != nil {
		return err
	}

	// n contains the number of rows that were deleted, so if it's 0, there was
	// no vehicle
	n, err := result.RowsAffected()
	if err != nil {
		return err
	}
	if n == 0 {
		return shuttletracker.ErrVehicleNotFound
	}
	// If this function is successful, it should return "nil"
	return nil
}

// Vehicle returns a Vehicle by its ID.
func (v *VehicleService) Vehicle(id int64) (*shuttletracker.Vehicle, error) {
	vehicle := &shuttletracker.Vehicle{
		ID: id,
	}

	// Finds the shuttle based on the input ID
	statement := "SELECT name, created, updated, enabled, tracker_id " +
		"FROM vehicles WHERE id = $1;"
	row := v.db.QueryRow(statement, id)
	err := row.Scan(&vehicle.Name, &vehicle.Created, &vehicle.Updated, &vehicle.Enabled, &vehicle.TrackerID)
	if err == sql.ErrNoRows {
		return vehicle, shuttletracker.ErrVehicleNotFound
	}

	return vehicle, err
}

// Vehicles returns all Vehicles.
func (v *VehicleService) Vehicles() ([]*shuttletracker.Vehicle, error) {
	// Vehicles list to be returned
	var vehicles []*shuttletracker.Vehicle
	// Postgres command that gets all vehicles
	statement := "SELECT id, name, created, updated, enabled, tracker_id FROM vehicles;"
	rows, err := v.db.Query(statement)
	if err != nil {
		return vehicles, err
	}

	// Loops through everything in "rows", which contains all vehicles pulled
	// from the database
	for rows.Next() {
		vehicle := &shuttletracker.Vehicle{}
		err := rows.Scan(&vehicle.ID, &vehicle.Name, &vehicle.Created, &vehicle.Updated, &vehicle.Enabled, &vehicle.TrackerID)
		if err != nil {
			return vehicles, err
		}
		// Appends the vehicle in this row to the return list if there is no err
		vehicles = append(vehicles, vehicle)
	}

	return vehicles, nil
}

// EnabledVehicles returns all Vehicles that are enabled.
func (v *VehicleService) EnabledVehicles() ([]*shuttletracker.Vehicle, error) {
	var vehicles []*shuttletracker.Vehicle

	// Postgres command that gets all vehicels with the var enabled set to true
	statement := "SELECT id, name, created, updated, tracker_id " +
		"FROM vehicles WHERE enabled = true;"
	rows, err := v.db.Query(statement)
	if err != nil {
		return vehicles, err
	}

	// Loops through everything in "rows", which contains all enabled vehicles
	// pulled from the database
	for rows.Next() {
		vehicle := &shuttletracker.Vehicle{
			Enabled: true,
		}
		err := rows.Scan(&vehicle.ID, &vehicle.Name, &vehicle.Created, &vehicle.Updated, &vehicle.TrackerID)
		if err != nil {
			return vehicles, err
		}
		// Appends the vehicle in this row to the return list if there is no err
		vehicles = append(vehicles, vehicle)
	}

	return vehicles, nil
}

// ModifyVehicle updates a Vehicle by its ID.
func (v *VehicleService) ModifyVehicle(vehicle *shuttletracker.Vehicle) error {
	// Updates the vehicle from the parameter "vehicle", referenced from $_
	statement := "UPDATE vehicles SET name = $1, enabled = $2, tracker_id = $3, updated = now() " +
		"WHERE id = $4 RETURNING updated;"
	row := v.db.QueryRow(statement, vehicle.Name, vehicle.Enabled, vehicle.TrackerID, vehicle.ID)
	err := row.Scan(&vehicle.Updated)
	return err
}

// VehicleWithTrackerID returns the Vehicle with the specified tracker ID.
func (v *VehicleService) VehicleWithTrackerID(id string) (*shuttletracker.Vehicle, error) {
	vehicle := &shuttletracker.Vehicle{
		TrackerID: id,
	}
	statement := "SELECT id, name, created, updated, enabled " +
		"FROM vehicles WHERE tracker_id = $1;"
	row := v.db.QueryRow(statement, id)
	err := row.Scan(&vehicle.ID, &vehicle.Name, &vehicle.Created, &vehicle.Updated, &vehicle.Enabled)
	if err == sql.ErrNoRows {
		return vehicle, shuttletracker.ErrVehicleNotFound
	}

	return vehicle, err
}
