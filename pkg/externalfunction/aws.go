package externalfunction

import (
	"encoding/base64"
	"fmt"
	"io/ioutil"
	"log"
	"os"
	"strings"
	"time"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/awserr"
	"github.com/aws/aws-sdk-go/aws/credentials"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/apigateway"
	"github.com/aws/aws-sdk-go/service/iam"
	"github.com/aws/aws-sdk-go/service/lambda"
	"github.com/aws/aws-sdk-go/service/sts"
	"github.com/tampajohn/goflake/pkg/common"
)

type AWSConfig struct {
	awsSession       *session.Session
	awsAccount       string
	accessKeyID      string
	secretAccessKey  string
	region           string
	Resources        *AWSResources
	extFuncName      string
	extFuncSignature string
}

type AWSResources struct {
	lambdaFuncName         string
	lambdaFuncARN          string
	lambdaPolicyName       string
	lambdaRoleName         string
	lambdaRoleARN          string
	lambdaRuntime          string
	lambdaHandler          string
	gatewayPolicyName      string
	gatewayRoleName        string
	gatewayRoleARN         string
	gatewayEndpoint        string
	gatewayID              string
	gatewayName            string
	gatewayMethod          string
	gatewayRootResource    string
	gatewayDeploymentID    string
	gatewayStage           string
	lambdaFunctionZipBytes []byte
	regionConfig           *aws.Config
}

const (
	LambdaZip     = `UEsDBBQAAAAIAIOOO1J7JNvACAMAAJ0HAAASABwAbGFtYmRhX2Z1bmN0aW9uLnB5VVQJAAOW7hFgDDQUYHV4CwABBPUBAAAEFAAAAJVVTWvcMBC9768Y3ENsWExaelrIIYSEtoc0pEsvy2JkezZWaktGkpMsIf+9M5K9/khL6bJgWzN682beaCSbVhsHj1ar1arEA9SiyUuRVUKVNZoYn1C5NRRaOXxxyWa1Avp9gE/n5yAtuArhy3Z7B9YJ11nyKxEO2kCkf0Wp9w2WzFsueN8AsaW9Bl1nFDyJukN4lnXtIwmpQNDfGHEEfQgvFmKtEKRSaHpTS29StZ0Do5+TEM5bMn3IaMlmTmd9iAvYwT6Edua48S+Bx43RjU8kQLXCiAYdQSt6lhD5EkRreEDn3XJdHtfwXMmiGtjaCdyIxAzSk8XDZLyZuPiPXcRfUc8q7L7S6gmNm6AcmJ6Abz++31ItjVQPZHB6WNL5IxZuDNOKY61FSTFY05TfbTzGTiaxthUpSP9cWFmIuj7+oeipl2la9CFlpjgBo2RBdU2OZk0wJdELonIzoKBSjXVthbVUWMqBszx0qnCSqJ7AuG7Ev89kF5XCiXmVbgbQU6HpzaNNa5KOkMyi92L0UX4Pd49UsJlufSoQ86ru3GLZd2qOfo+lrJJ0CsieWe95wR+78/3JPnNkvekA2oDUxyE5qDpajYzSIJWVTVsj4Ivg5wKoQYOkIBaVRjtJJmfF6prqwZ3D614X2zfRILirhFsgnpI0KBwxEpZEtYRCHH5e3n+9vN0GrHnyIYssqE+nLromSqFrad1uov3MffbBMkkvklAPGNeoYi7fx80+SWaavYuUirZFVXp3uU9Wi1zuunCkwjCgZCZicrfObIH6rD7v9Z1NllHw9YzVfsmDejKcjrL0JR+qH45C4wff+3h/mWmTnMfFSeo8AGjuNq1wMq8xC9NjxtzPiLJrWhu/hnMGm7+Fe+uh8aXA1sG1f9DB5cZAY6Yj9TPfDtSkkvrM6oZ66NgiDxXy02ZMbn430K7pdEK+DWgs8XnjUjk+A8O0oKbu6hKUdhCuqvR/0h7n4XAZ3QeTW95JLNbygguherfXU9yz4HJFHmebaWrr0YVDkvFfFP2Gt9VvUEsBAh4DFAAAAAgAg447Unsk28AIAwAAnQcAABIAGAAAAAAAAQAAAKSBAAAAAGxhbWJkYV9mdW5jdGlvbi5weVVUBQADlu4RYHV4CwABBPUBAAAEFAAAAFBLBQYAAAAAAQABAFgAAABUAwAAAAA=`
	TrustDocument = `{
		"Version": "2012-10-17",
		"Statement": {
		  "Effect": "Allow",
		  "Principal": {"Service": "lambda.amazonaws.com"},
		  "Action": "sts:AssumeRole"
		}
	  }`

	ApiResourcePolicy = `{
		"Version": "2012-10-17",
		"Statement": [
			{
				"Effect": "Allow",
				"Principal": {
					"AWS": "arn:aws:sts::%s:assumed-role/%s/snowflake"
				},
				"Action": "execute-api:Invoke",
				"Resource": "arn:aws:execute-api:%s:%s:%s/*"
			}
		]
	}`

	ExternalApiRoleTrustDocument = `{
		"Version": "2012-10-17",
		"Statement":
		[
			{
			"Effect": "Allow",
			"Principal":
				{
				"AWS": "%s"
				},
			"Condition": {
				"StringEquals": {
					"sts:ExternalId": "%s"
				}
			},
			"Action": "sts:AssumeRole"
			}
		]
	}`

	InvokeExternalApiPolicyDocument = `{
		"Version": "2012-10-17",
		"Statement": [
			{
				"Effect": "Allow",
				"Action": [
					"execute-api:Invoke"
				],
				"Resource": "arn:aws:execute-api:%s:%s:%s/*"
			}
		]
	}`

	BasicRoleTemplate = `{
		"Version": "2012-10-17",
		"Statement": [
		  {
			"Effect": "Allow",
			"Action": [
			  "logs:CreateLogGroup",
			  "logs:CreateLogStream",
			  "logs:PutLogEvents"
			],
			"Resource": "arn:aws:logs:*:*:*"
		  }
		]
	  }
	  `
)

func NewAWSConfig(extFuncName string, extFuncSignature string) (*AWSConfig, error) {
	cfg := &AWSConfig{Resources: &AWSResources{}}
	cfg.extFuncName = extFuncName
	cfg.extFuncSignature = extFuncSignature

	if common.AskYesNo("Would you like to us to attempt to use your AWS_ACCESS_KEY_ID from your environment?") {
		// Attempt to get the aws creds from ENV; fail back to prompting the user
		cfg.accessKeyID = common.EnvOrString("AWS_ACCESS_KEY_ID", false)
		cfg.secretAccessKey = common.EnvOrString("AWS_SECRET_ACCESS_KEY", true)
		cfg.region = common.EnvOrString("AWS_DEFAULT_REGION", false)
	} else {
		// Just get the creds from the user
		cfg.accessKeyID = common.PromptString("AWS_ACCESS_KEY_ID", false, "")
		cfg.secretAccessKey = common.PromptString("AWS_SECRET_ACCESS_KEY", true, "")
		cfg.region = common.PromptString("AWS_DEFAULT_REGION", false, "")
	}

	// set AWS env variables in this proc
	os.Setenv("AWS_ACCESS_KEY_ID", cfg.accessKeyID)
	os.Setenv("AWS_SECRET_ACCESS_KEY", cfg.secretAccessKey)
	os.Setenv("AWS_DEFAULT_REGION", cfg.region)

	cfg.Resources.regionConfig = &aws.Config{Region: &cfg.region}

	sess, err := session.NewSession(&aws.Config{
		Credentials: credentials.NewEnvCredentials(),
	})

	if err != nil {
		return nil, err
	}

	cfg.awsSession = sess
	cfg.extFuncName = extFuncName
	cfg.Resources.lambdaRoleName = common.PromptString(
		"What would you like the lambda role to be named?",
		false,
		extFuncName+"-lambda-role")
	cfg.Resources.lambdaFuncName = common.PromptString(
		"What would you like the lambda to be named?",
		false,
		extFuncName+"-lambda")
	cfg.Resources.gatewayName = common.PromptString(
		"What would you like the api gateway to be named?",
		false,
		extFuncName+"-gateway")
	cfg.Resources.lambdaPolicyName = common.PromptString(
		"What would you like the lambda policy to be named?",
		false,
		extFuncName+"-lambda-policy")
	cfg.Resources.lambdaRuntime = common.PromptString(
		"What lambda runtime would you like to use?",
		false,
		lambda.RuntimePython38)
	cfg.Resources.gatewayPolicyName = common.PromptString(
		"What would you like the gateway policy to be named?",
		false,
		extFuncName+"-gateway-policy")
	cfg.Resources.gatewayRoleName = common.PromptString(
		"What would you like the gateway role to be named?",
		false,
		extFuncName+"-gateway-role")
	cfg.Resources.gatewayStage = common.PromptString(
		"What would you like the gateway stage to be named?",
		false,
		"prod")

	if common.AskYesNo("Would you like to use the default lambda function?") {
		cfg.Resources.lambdaHandler = "lambda_function.lambda_handler"

		functionData, err := base64.StdEncoding.DecodeString(LambdaZip)
		if err != nil {
			return nil, err
		}

		cfg.Resources.lambdaFunctionZipBytes = functionData
	} else {
		zipPath := common.PromptStringWithValidator("What is the path of the zip file you'd like to use?", false, "", func(p string) error {
			info, err := os.Stat(p)
			if os.IsNotExist(err) {
				return err
			}
			if info.IsDir() {
				return fmt.Errorf("This is a directory, not a file")
			}
			return nil
		})
		f, err := os.Open(zipPath)
		defer f.Close()
		if err != nil {
			return cfg, err
		}
		bytes, err := ioutil.ReadAll(f)
		if err != nil {
			return cfg, err
		}
		handler := common.PromptString("What is the handler for your lambda function (format is {filename}.{handler function})?", false, "lambda_function.lambda_handler")
		cfg.Resources.lambdaHandler = handler
		cfg.Resources.lambdaFunctionZipBytes = bytes
	}

	return cfg, nil
}

func APIARN(apiID *string, functionARN *string, functionName *string) string {
	apiArn := strings.Replace(aws.StringValue(functionARN), "lambda", "execute-api", 1)
	return strings.Replace(apiArn,
		fmt.Sprintf("function:%s", aws.StringValue(functionName)),
		aws.StringValue(apiID), 1)
}

func (cfg *AWSConfig) CreateLambdaRole(a *iam.IAM) error {
	roleInput := &iam.CreateRoleInput{
		RoleName:                 aws.String(cfg.Resources.lambdaRoleName),
		AssumeRolePolicyDocument: aws.String(TrustDocument),
	}
	if common.AskYesNo("Do you wish to include a Permission Boundary?") {
		permissionBoundary := common.PromptString("What is the ARN of the boundary you'd like to attach to this role?", false, "")
		roleInput.SetPermissionsBoundary(permissionBoundary)
	}
	_, err := a.CreateRole(roleInput)

	if err == nil {
		fmt.Println("Waiting 15s for role to propagate")
		time.Sleep(15 * time.Second)
	}

	putParams := &iam.PutRolePolicyInput{
		PolicyDocument: aws.String(BasicRoleTemplate),
		PolicyName:     aws.String(cfg.Resources.lambdaPolicyName),
		RoleName:       aws.String(cfg.Resources.lambdaRoleName),
	}
	_, err = a.PutRolePolicy(putParams)

	if err != nil {
		return err
	}
	return nil
}

func (cfg *AWSConfig) SetCurrentAccountID() error {
	s := sts.New(cfg.awsSession)
	id, err := s.GetCallerIdentity(&sts.GetCallerIdentityInput{})

	if err != nil {
		return err
	}
	cfg.awsAccount = *id.Account
	return nil
}

func (cfg *AWSConfig) CreateOrConfigureLambdaFunc(a *iam.IAM) error {
	lrole, err := a.GetRole(&iam.GetRoleInput{
		RoleName: aws.String(cfg.Resources.lambdaRoleName),
	})

	cfg.Resources.lambdaRoleARN = *lrole.Role.Arn
	l := lambda.New(cfg.awsSession, cfg.Resources.regionConfig)

	lf, err := l.CreateFunction(&lambda.CreateFunctionInput{
		FunctionName: aws.String(cfg.Resources.lambdaFuncName),
		Role:         aws.String(cfg.Resources.lambdaRoleARN),
		Runtime:      aws.String(cfg.Resources.lambdaRuntime),
		Handler:      aws.String(cfg.Resources.lambdaHandler),
		Code: &lambda.FunctionCode{
			ZipFile: cfg.Resources.lambdaFunctionZipBytes,
		},
	})

	// Will not recreate the lambda function
	if err != nil &&
		err.(awserr.Error).Code() != lambda.ErrCodeResourceConflictException {
		return err
	}
	if lf != nil {
		cfg.Resources.lambdaFuncARN = *lf.FunctionArn
	}
	if err != nil {
		lf2, err := l.GetFunction(&lambda.GetFunctionInput{
			FunctionName: aws.String(cfg.Resources.lambdaFuncName),
		})

		if err != nil {
			return err
		}
		cfg.Resources.lambdaFuncARN = *lf2.Configuration.FunctionArn
	}
	permissionsInput := &lambda.AddPermissionInput{
		Action:       aws.String("lambda:InvokeFunction"),
		FunctionName: aws.String(cfg.Resources.lambdaFuncName),
		Principal:    aws.String("apigateway.amazonaws.com"),
		StatementId: aws.String(fmt.Sprintf("apigateway-%s-test",
			cfg.Resources.gatewayID)),
		SourceArn: aws.String(fmt.Sprintf("%s/*/%s/",
			APIARN(aws.String(cfg.Resources.gatewayID), aws.String(cfg.Resources.lambdaFuncARN), aws.String(cfg.Resources.lambdaFuncName)),
			cfg.Resources.gatewayMethod)),
	}
	_, err = l.AddPermission(permissionsInput)

	if err != nil {
		return err
	}

	return nil
}

func (cfg *AWSConfig) CreateRestAPI(g *apigateway.APIGateway) error {
	gw, err := g.CreateRestApi(&apigateway.CreateRestApiInput{
		Name: aws.String(cfg.Resources.gatewayName),
	})

	if err != nil {
		return err
	}

	cfg.Resources.gatewayID = *gw.Id
	cfg.Resources.gatewayEndpoint = fmt.Sprintf("https://%s.execute-api.%s.amazonaws.com/%s/",
		cfg.Resources.gatewayID, cfg.region, cfg.Resources.gatewayStage)

	r2, err := g.GetResources(&apigateway.GetResourcesInput{
		RestApiId: gw.Id,
	})
	cfg.Resources.gatewayMethod = "POST"

	var rootResource *apigateway.Resource
	if len(r2.Items) > 0 {
		rootResource = r2.Items[0]
		cfg.Resources.gatewayRootResource = *rootResource.Id
	}
	return nil
}

func (cfg *AWSConfig) AddLambdaIntegrationToRestAPI(g *apigateway.APIGateway) error {
	_, err := g.PutMethod(&apigateway.PutMethodInput{
		HttpMethod:        aws.String(cfg.Resources.gatewayMethod),
		RestApiId:         aws.String(cfg.Resources.gatewayID),
		ResourceId:        aws.String(cfg.Resources.gatewayRootResource),
		AuthorizationType: aws.String("AWS_IAM"),
	})

	if err != nil {
		return err
	}

	uriString := fmt.Sprintf("arn:aws:apigateway:%s:lambda:path/2015-03-31/functions/%s/invocations",
		cfg.region,
		cfg.Resources.lambdaFuncARN)

	params := &apigateway.PutIntegrationInput{
		HttpMethod:            aws.String(cfg.Resources.gatewayMethod),
		ResourceId:            aws.String(cfg.Resources.gatewayRootResource),
		RestApiId:             aws.String(cfg.Resources.gatewayID),
		Type:                  aws.String("AWS_PROXY"),
		IntegrationHttpMethod: aws.String(cfg.Resources.gatewayMethod),
		RequestTemplates: map[string]*string{
			"application/x-www-form-urlencoded": aws.String(`{"body": $input.json("$")}`),
		},
		Uri: aws.String(uriString),
	}
	_, err = g.PutIntegration(params)

	if err != nil {
		return err
	}

	integrationResponseParams := &apigateway.PutIntegrationResponseInput{
		HttpMethod:       aws.String(cfg.Resources.gatewayMethod),
		ResourceId:       aws.String(cfg.Resources.gatewayRootResource),
		RestApiId:        aws.String(cfg.Resources.gatewayID),
		StatusCode:       aws.String("200"),
		SelectionPattern: aws.String(".*"),
	}
	_, err = g.PutIntegrationResponse(integrationResponseParams)

	if err != nil {
		return err
	}

	methodResponsParams := &apigateway.PutMethodResponseInput{
		HttpMethod:     aws.String(cfg.Resources.gatewayMethod),
		ResourceId:     aws.String(cfg.Resources.gatewayRootResource),
		RestApiId:      aws.String(cfg.Resources.gatewayID),
		StatusCode:     aws.String("200"),
		ResponseModels: map[string]*string{},
	}
	_, err = g.PutMethodResponse(methodResponsParams)

	return err
}

func (cfg *AWSConfig) ConfigureAwsRoles() error {
	err := cfg.SetCurrentAccountID()
	if err != nil {
		return err
	}

	a := iam.New(cfg.awsSession)
	err = cfg.CreateLambdaRole(a)
	if err != nil {
		return err
	}

	g := apigateway.New(cfg.awsSession, cfg.Resources.regionConfig)
	err = cfg.CreateRestAPI(g)
	if err != nil {
		return err
	}

	err = cfg.CreateOrConfigureLambdaFunc(a)
	if err != nil {
		return err
	}

	err = cfg.AddLambdaIntegrationToRestAPI(g)
	return err
}

func (scfg *AWSConfig) EnsureRole(roleName string, policyDocument string) (arn string) {
	i := iam.New(scfg.awsSession)
	roleInput := &iam.CreateRoleInput{
		RoleName:                 aws.String(roleName),
		AssumeRolePolicyDocument: aws.String(policyDocument),
	}
	if common.AskYesNo("Do you wish to include a Permission Boundary?") {
		permissionBoundary := common.PromptString("What is the ARN of the boundary you'd like to attach to this role?", false, "")
		roleInput.SetPermissionsBoundary(permissionBoundary)
	}
	r, err := i.CreateRole(roleInput)

	// Role successfully created
	if err == nil {
		return *r.Role.Arn
	} else {
		fmt.Println(err)
	}

	// Get existing Role
	xr, err := i.GetRole(&iam.GetRoleInput{
		RoleName: aws.String(roleName),
	})

	if err != nil {
		log.Fatalf("Not able to ensure role %s, encountered error: %s", roleName, err)
		return
	}

	_, err = i.UpdateAssumeRolePolicy(&iam.UpdateAssumeRolePolicyInput{
		PolicyDocument: aws.String(policyDocument),
		RoleName:       aws.String(roleName),
	})

	if err == nil {
		return *xr.Role.Arn
	}

	log.Fatalf("Not able to ensure role %s", roleName)
	return
}

func (scfg *SnowflakeConfig) AddTrustToAWSRole() error {
	g := apigateway.New(scfg.awsSession, &aws.Config{Region: &scfg.region})
	i := iam.New(scfg.awsSession)

	scfg.EnsureRole(scfg.Resources.gatewayRoleName,
		fmt.Sprintf(ExternalApiRoleTrustDocument,
			scfg.iamUserARN,
			scfg.apiExternalID))

	pd := fmt.Sprintf(InvokeExternalApiPolicyDocument,
		scfg.region,
		scfg.awsAccount,
		scfg.Resources.gatewayID)

	putParams := &iam.PutRolePolicyInput{
		PolicyDocument: aws.String(pd),
		PolicyName:     aws.String(scfg.Resources.gatewayPolicyName),
		RoleName:       aws.String(scfg.Resources.gatewayRoleName),
	}
	_, err := i.PutRolePolicy(putParams)

	fmt.Println("Waiting 15s for policy to propagate...")

	time.Sleep(15 * time.Second)

	if err != nil {
		return err
	}

	pd = fmt.Sprintf(ApiResourcePolicy,
		scfg.awsAccount,
		scfg.Resources.gatewayRoleName,
		scfg.region,
		scfg.awsAccount,
		scfg.Resources.gatewayID)

	_, err = g.UpdateRestApi(&apigateway.UpdateRestApiInput{
		RestApiId: aws.String(scfg.Resources.gatewayID),
		PatchOperations: []*apigateway.PatchOperation{
			{
				Op:    aws.String("replace"),
				Path:  aws.String("/policy"),
				Value: aws.String(pd),
			},
		},
	})

	if err != nil {
		return err
	}

	scfg.executeSnowflakeQuery(fmt.Sprintf(`create or replace external function %s
    returns variant
    api_integration = %s_api_integration
    as '%s'
	;`, scfg.extFuncSignature,
		scfg.extFuncName,
		scfg.Resources.gatewayEndpoint), func(scan func(dest ...interface{}) error) error {
		var s string = ""
		err = scan(&s)
		if err != nil {
			return err
		}
		fmt.Print(s)
		return nil
	})

	_, err = g.CreateDeployment(&apigateway.CreateDeploymentInput{
		RestApiId: aws.String(scfg.Resources.gatewayID),
		StageName: aws.String(scfg.Resources.gatewayStage),
	})

	if err != nil {
		return err
	}

	return nil
}

func (cfg *AWSConfig) DeleteGateways() {

}
