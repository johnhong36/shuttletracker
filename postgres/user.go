package postgres

import (
	"database/sql"

	"github.com/wtg/shuttletracker"
)

// UserService is an implementation of shuttletracker.UserService.
type UserService struct {
	db *sql.DB
}

// Initializes how the data is represented in the Postgres database
func (us *UserService) initializeSchema(db *sql.DB) error {
	us.db = db
	schema := `
CREATE TABLE IF NOT EXISTS users (
	id serial PRIMARY KEY,
	username varchar(10) UNIQUE NOT NULL
);
	`
	_, err := us.db.Exec(schema)
	return err
}

// CreateUser creates a User.
func (us *UserService) CreateUser(user *shuttletracker.User) error {
	statement := "INSERT INTO users (username) " +
		"VALUES ($1) RETURNING id;"
	row := us.db.QueryRow(statement, user.Username)
	// If this function is successful, it should return "nil"
	err := row.Scan(&user.ID)
	return err
}

// DeleteUser deletes a User by its username.
func (us *UserService) DeleteUser(username string) error {
	statement := "DELETE FROM users WHERE username = $1;"
	result, err := us.db.Exec(statement, username)
	if err != nil {
		return err
	}

	// n contains the number of rows that were deleted, so if it's 0, there was
	// no user
	n, err := result.RowsAffected()
	if err != nil {
		return err
	}
	if n == 0 {
		return shuttletracker.ErrUserNotFound
	}
	// If this function is successful, it should return "nil"
	return nil
}

// Users returns all existing Users..
func (us *UserService) Users() ([]*shuttletracker.User, error) {
	// Users list to be returned
	var users []*shuttletracker.User
	// Postgres command that gets all users
	statement := "SELECT id, username FROM users;"
	rows, err := us.db.Query(statement)
	if err != nil {
		return users, err
	}

	// Loops through everything in "rows", which contains all users pulled from
	// the database
	for rows.Next() {
		user := &shuttletracker.User{}
		err := rows.Scan(&user.ID, &user.Username)
		if err != nil {
			return users, err
		}
		// Aooends the users in this row to the return list if there is no err
		users = append(users, user)
	}

	return users, nil
}

// UserExists returns whether a User with the specified username exists.
func (us *UserService) UserExists(username string) (bool, error) {
	// Grabs username from input param, and returns true if no errors occur
	row := us.db.QueryRow("SELECT FROM users WHERE username = $1;", username)
	err := row.Scan()
	if err == sql.ErrNoRows {
		return false, nil
	} else if err != nil {
		return false, err
	}
	return true, nil
}
