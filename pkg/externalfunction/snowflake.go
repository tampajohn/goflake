package externalfunction

import (
	"database/sql"
	"fmt"
	"log"

	sf "github.com/snowflakedb/gosnowflake"
	"github.com/tampajohn/goflake/pkg/common"
)

type SnowflakeConfig struct {
	*AWSConfig
	dsn       string
	sfAccount string
	sfUser    string
	sfPass    string

	apiExternalID string
	apiRoleARN    string
	iamUserARN    string
}

func NewSnowflakeConfig(awsCfg *AWSConfig) *SnowflakeConfig {
	cfg := &SnowflakeConfig{AWSConfig: awsCfg}
	if common.AskYesNo("Would you like to us to attempt to use your SNOWFLAKE_[ACCOUNT|USER|PASS] from your environment?") {
		// Attempt to get the aws creds from ENV; fail back to prompting the user
		cfg.sfAccount = common.EnvOrString("SNOWFLAKE_ACCOUNT", false)
		cfg.sfUser = common.EnvOrString("SNOWFLAKE_USER", false)
		cfg.sfPass = common.EnvOrString("SNOWFLAKE_PASS", true)
	} else {
		// Just get the creds from the user
		cfg.sfAccount = common.PromptString("SNOWFLAKE_ACCOUNT", false, "")
		cfg.sfUser = common.PromptString("SNOWFLAKE_USER", false, "")
		cfg.sfPass = common.PromptString("SNOWFLAKE_PASS", true, "")
	}

	database := common.PromptString("What database would you like to use?", false, "")
	role := common.PromptString("What Snowflake Role do you wish to use (requires ability to create integrations)?", false, "ACCOUNT_ADMIN")
	schema := common.PromptString("What schema would you like the external function created in?", false, "PUBLIC")

	dsn, err := sf.DSN(&sf.Config{
		Account:  cfg.sfAccount,
		User:     cfg.sfUser,
		Password: cfg.sfPass,
		Host:     cfg.sfAccount + ".snowflakecomputing.com",
		Database: database,
		Port:     443,
		Role:     role,
		Schema:   schema,
		Protocol: "https",
	})

	cfg.dsn = dsn
	logger := sf.CreateDefaultLogger()
	logger.SetLogLevel("panic")
	sf.SetLogger(&logger)

	var s string
	err = cfg.executeSnowflakeQuery(fmt.Sprintf(`create or replace api integration %s_api_integration
	api_provider = aws_api_gateway
	api_aws_role_arn = '%s'
	api_allowed_prefixes = ('%s')
	enabled = true;`,
		cfg.extFuncName,
		cfg.EnsureRole(cfg.Resources.gatewayRoleName, TrustDocument),
		cfg.Resources.gatewayEndpoint), func(scan func(dest ...interface{}) error) error {
		return scan(&s)
	})
	if err != nil {
		log.Fatalf("Error encountered: %v", err)
	}
	// fmt.Println(s)

	type describeResults struct {
		Property     string
		PropertyType string
		Value        string
		Default      string
	}
	err = cfg.executeSnowflakeQuery(fmt.Sprintf(`describe integration %s_api_integration;`, cfg.extFuncName), func(scan func(dest ...interface{}) error) error {
		r := &describeResults{
			Value:   "",
			Default: "",
		}
		err = scan(&r.Property, &r.PropertyType, &r.Value, &r.Default)
		if err != nil {
			return err
		}
		switch r.Property {
		case "API_AWS_EXTERNAL_ID":
			cfg.apiExternalID = r.Value
		case "API_AWS_ROLE_ARN":
			cfg.apiRoleARN = r.Value
		case "API_AWS_IAM_USER_ARN":
			cfg.iamUserARN = r.Value
		}
		return nil
	})
	return cfg
	// set AWS env variables in this proc
}

func (cfg *SnowflakeConfig) executeSnowflakeQuery(query string, scanner func(func(dest ...interface{}) error) error) error {
	db, err := sql.Open("snowflake", cfg.dsn)
	if err != nil {
		log.Fatalf("failed to connect. %v, err: %v", cfg.dsn, err)
	}
	defer db.Close()
	rows, err := db.Query(query) // no cancel is allowed
	if err != nil {
		log.Fatalf("failed to run a query. %v, err: %v", query, err)
	}
	defer rows.Close()
	for rows.Next() {
		err := scanner(rows.Scan)
		if err != nil {
			log.Fatalf("failed to scan values: %v", err)
		}
	}
	if rows.Err() != nil {
		fmt.Printf("ERROR: %v\n", rows.Err())
		return rows.Err()
	}
	fmt.Printf("Congrats! You have successfully run %v with Snowflake DB!\n", query)
	return nil
}
