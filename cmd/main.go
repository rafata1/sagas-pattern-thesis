package main

import (
	"fmt"
	"github.com/golang-migrate/migrate/v4"
	_ "github.com/golang-migrate/migrate/v4/database/mysql"
	_ "github.com/golang-migrate/migrate/v4/source/file"
	"github.com/rafata1/sagas-pattern-thesis/config"
	"github.com/spf13/cobra"
	"os"
	"time"
)

const versionTimeFormat = "20060102150405"

func main() {
	rootCmd := &cobra.Command{}
	rootCmd.AddCommand(
		createMigrationCommand(),
		migrateCommand(),
	)

	err := rootCmd.Execute()
	if err != nil {
		panic(err)
	}
}

func createMigrationCommand() *cobra.Command {
	return &cobra.Command{
		Use:   "migrate-create [service] [name]",
		Short: "create sql migrations",
		Args:  cobra.ExactArgs(2),
		Run: func(cmd *cobra.Command, args []string) {
			now := time.Now()
			version := now.Format(versionTimeFormat)
			service := args[0]
			name := args[1]
			migrationDir, _ := getMigrationAndDatabase(service, config.DefaultConfig)
			up := fmt.Sprintf("%s/%s_%s.up.sql", migrationDir, version, name)
			down := fmt.Sprintf("%s/%s_%s.down.sql", migrationDir, version, name)

			err := os.WriteFile(up, []byte{}, 0644)
			if err != nil {
				panic(err)
			}

			err = os.WriteFile(down, []byte{}, 0644)
			if err != nil {
				panic(err)
			}

			fmt.Println("Created SQL up script:", up)
			fmt.Println("Created SQL down script:", down)
		},
	}
}

func migrateCommand() *cobra.Command {
	return &cobra.Command{
		Use:   "migrate-up [service]",
		Short: "migrate all the way up",
		Args:  cobra.ExactArgs(1),
		Run: func(cmd *cobra.Command, args []string) {
			service := args[0]

			migrationDir, databaseDSN := getMigrationAndDatabase(service, config.DefaultConfig)
			m, err := migrate.New(
				fmt.Sprintf("file://%s", migrationDir),
				fmt.Sprintf("mysql://%s", databaseDSN),
			)
			if err != nil {
				panic(err)
			}

			err = m.Up()
			if err == migrate.ErrNoChange {
				fmt.Println("No change in migration")
				return
			}
			if err != nil {
				panic(err)
			}
			fmt.Println("Migrated up")
		},
	}
}

func getMigrationAndDatabase(service string, conf config.Config) (string, string) {
	if service == conf.OrderConfig.Name {
		return conf.OrderConfig.MigrationDir, conf.OrderConfig.DatabaseDSN
	}
	if service == conf.InventoryConfig.Name {
		return conf.InventoryConfig.MigrationDir, conf.InventoryConfig.DatabaseDSN
	}
	if service == conf.PaymentConfig.Name {
		return conf.PaymentConfig.MigrationDir, conf.PaymentConfig.DatabaseDSN
	}
	return "", ""
}
