package externalfunction

import (
	"encoding/base64"
	"fmt"
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
	awsSession      *session.Session
	awsAccount      string
	accessKeyID     string
	secretAccessKey string
	region          string
	Resources       *AWSResources
	extFuncName     string
}

type AWSResources struct {
	lambdaFuncName      string
	lambdaPolicyName    string
	lambdaRoleName      string
	lambdaRoleARN       string
	lambdaRuntime       string
	lambdaHandler       string
	gatewayPolicyName   string
	gatewayRoleName     string
	gatewayRoleARN      string
	gatewayEndpoint     string
	gatewayID           string
	gatewayDeploymentID string
	gatewayStage        string
}

const (
	LambdaZip     = `UEsDBBQAAAAIAHN7OVIrnQNoRAMAAJAIAAASABwAbGFtYmRhX2Z1bmN0aW9uLnB5VVQJAAO6KQ9gvCkPYHV4CwABBPUBAAAEFAAAAJVVTWvcMBC9768Q7iG7YEwSelrIIYSEtoc0pEsvSzGyNc4qtSUjyUmW0P/eGUlef2yW0sWwtjR68+bNh2TTauPYs9VqsRBQsZo3heD5jitRg1nCCyiXslIrB29utV4sGP4+scvzcyYtcztgXzabB2Ydd51FOwGs0oYl+neSeduwk/udKzrXQ2zwrAHXGcVeeN0Be5V17T1xqRjHxxi+Z7oKL5YttQImlQITt1p8k6rtHDP6dRXc+Z1cVzku2dzpPLq4Ylv2K7h2Zr/2L4HHndGNDyRAtdzwBhxCK/wXLPESJCl7AufNCi32KXvdyXLXs7UjuAGJGGSHHQ+T02Hk4j+2CX0lkVU4faPVCxg3QqmIHmfffny/Ry2NVE+44XS/pItnKN3gpuX7WnOBPiinGb3b5eB7NfK12WEG8Sm4lSWv6/0Homc+TWPR+5CJ4ggMg2WqawowKcIIpBeSSsUAHKUadG25tSgsxkBRVp0qnUSqBzDSDfnHSLaJ4I5PVbrrQQ9C45tHG2uSDZDEIloR+pD+gPcIqNgkcTEWtqRV3bnZsi/VAvwZi2GtsgkiWubR8oo+tucj+jOXlTTWzWvvzAb5shDqG2/aGlK0x3SVmCV0zWeA6A+MLKPseIrHcknpQ7rDOcwgNn2HOQqmtkMhuZ3BDRXXldhAMA3Q0839+fwixnhxOkYLWDXiZJAnoS8j9OUR9A3FYEMCYnqwjLGqtBoSmYUSt5LU61WcAaFogJUP5U6DHdVAQZVe11hH1HG07inZ2Hx9o7gddzPEQ20Y4A4ZcUupQBTk8PP68ev1/eajuEMUIXCaVsktUgrdjut2jRNoovr081ihhy5MkTD/kMeofKlBJ3vB6yS044qeDNOhxNMJ9SMe2IZhIAjh1eqFC93f+Fl/7O/EGM9424ISywmd1eCSZl5O9c2dLGrIQwdMmPuxKLqmtcv3MFrY+pS7PxEa3kpoHbv1fzirKKdgzPgW+UwXItaXxBKxusH071ugOYp22gzBTa9DPDUeyEAXIE5imjAkle/3fkBiPXa1YEo7Fm7n7H/CHq6A/v59DFtufg1TsuZ3enAVzd4Pfs+CyQ1anK3HoaWDCbnEzX9R9Af+LP4CUEsBAh4DFAAAAAgAc3s5UiudA2hEAwAAkAgAABIAGAAAAAAAAQAAAKSBAAAAAGxhbWJkYV9mdW5jdGlvbi5weVVUBQADuikPYHV4CwABBPUBAAAEFAAAAFBLBQYAAAAAAQABAFgAAACQAwAAAAA=`
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

func NewAWSConfig(extFuncName string) (*AWSConfig, error) {
	cfg := &AWSConfig{Resources: &AWSResources{}}

	if common.AskYesNo("Would you like to us to attempt to use your AWS_ACCESS_KEY_ID from your environment?") {
		// Attempt to get the aws creds from ENV; fail back to prompting the user
		cfg.accessKeyID = common.EnvOrString("AWS_ACCESS_KEY_ID", false)
		cfg.secretAccessKey = common.EnvOrString("AWS_SECRET_ACCESS_KEY", true)
		cfg.region = common.EnvOrString("AWS_DEFAULT_REGION", false)
	} else {
		// Just get the creds from the user
		cfg.accessKeyID = common.PromptString("AWS_ACCESS_KEY_ID", false)
		cfg.secretAccessKey = common.EnvOrString("AWS_SECRET_ACCESS_KEY", true)
		cfg.region = common.EnvOrString("AWS_DEFAULT_REGION", false)
	}

	// set AWS env variables in this proc
	os.Setenv("AWS_ACCESS_KEY_ID", cfg.accessKeyID)
	os.Setenv("AWS_SECRET_ACCESS_KEY", cfg.secretAccessKey)
	os.Setenv("AWS_DEFAULT_REGION", cfg.region)

	sess, err := session.NewSession(&aws.Config{
		Credentials: credentials.NewEnvCredentials(),
	})

	if err != nil {
		return nil, err
	}

	cfg.awsSession = sess
	cfg.extFuncName = extFuncName
	cfg.Resources.lambdaRoleName = extFuncName + "-lambda-role"
	cfg.Resources.lambdaFuncName = extFuncName + "-lambda"
	cfg.Resources.lambdaPolicyName = extFuncName + "-lambda-policy"
	cfg.Resources.lambdaRuntime = lambda.RuntimePython38
	cfg.Resources.gatewayPolicyName = extFuncName + "-gateway-policy"
	cfg.Resources.gatewayRoleName = extFuncName + "-gateway-role"
	cfg.Resources.lambdaHandler = "lambda_function.lambda_handler"
	cfg.Resources.gatewayStage = "prod"

	return cfg, nil
}

func APIARN(apiID *string, functionARN *string, functionName *string) string {
	apiArn := strings.Replace(aws.StringValue(functionARN), "lambda", "execute-api", 1)
	return strings.Replace(apiArn,
		fmt.Sprintf("function:%s", aws.StringValue(functionName)),
		aws.StringValue(apiID), 1)
}

func (cfg *AWSConfig) ConfigureAwsRoles() error {
	regionConfig := &aws.Config{Region: &cfg.region}

	a := iam.New(cfg.awsSession)

	_, err := a.CreateRole(&iam.CreateRoleInput{
		RoleName:                 aws.String(cfg.Resources.lambdaRoleName),
		AssumeRolePolicyDocument: aws.String(TrustDocument),
	})

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

	s := sts.New(cfg.awsSession)
	id, err := s.GetCallerIdentity(&sts.GetCallerIdentityInput{})

	cfg.awsAccount = *id.Account

	lrole, err := a.GetRole(&iam.GetRoleInput{
		RoleName: aws.String(cfg.Resources.lambdaRoleName),
	})

	cfg.Resources.lambdaRoleARN = *lrole.Role.Arn
	l := lambda.New(cfg.awsSession, regionConfig)

	functionData, err := base64.StdEncoding.DecodeString(LambdaZip)
	if err != nil {
		return err
	}

	var functionARN *string
	lf, err := l.CreateFunction(&lambda.CreateFunctionInput{
		FunctionName: aws.String(cfg.Resources.lambdaFuncName),
		Role:         aws.String(cfg.Resources.lambdaRoleARN),
		Runtime:      aws.String(cfg.Resources.lambdaRuntime),
		Handler:      aws.String(cfg.Resources.lambdaHandler),
		Code: &lambda.FunctionCode{
			ZipFile: functionData,
		},
	})

	// Will not recreate the lambda function
	if err != nil &&
		err.(awserr.Error).Code() != lambda.ErrCodeResourceConflictException {
		return err
	}
	if lf != nil {
		functionARN = lf.FunctionArn
	}
	if err != nil {
		lf2, err := l.GetFunction(&lambda.GetFunctionInput{
			FunctionName: aws.String(cfg.Resources.lambdaFuncName),
		})

		if err != nil {
			return err
		}
		functionARN = lf2.Configuration.FunctionArn
	}

	g := apigateway.New(cfg.awsSession, regionConfig)

	gw, err := g.CreateRestApi(&apigateway.CreateRestApiInput{
		Name: aws.String(cfg.Resources.lambdaFuncName),
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
	method := aws.String("POST")

	var rootResource *apigateway.Resource
	if len(r2.Items) > 0 {
		rootResource = r2.Items[0]
	}

	permissionsInput := &lambda.AddPermissionInput{
		Action:       aws.String("lambda:InvokeFunction"),
		FunctionName: aws.String(cfg.Resources.lambdaFuncName),
		Principal:    aws.String("apigateway.amazonaws.com"),
		StatementId: aws.String(fmt.Sprintf("apigateway-%s-test",
			cfg.Resources.gatewayID)),
		SourceArn: aws.String(fmt.Sprintf("%s/*/%s/",
			APIARN(gw.Id, functionARN, aws.String(cfg.Resources.lambdaFuncName)),
			aws.StringValue(method))),
	}
	_, err = l.AddPermission(permissionsInput)

	if err != nil {
		return err
	}

	// TODO: this should be done after the deployment, might occur as part of deployment process...
	// permissionsInput.SourceArn = aws.String(cfg.Resources.gatewayEndpoint)
	// permissionsInput.StatementId = aws.String(fmt.Sprintf("apigateway-%s-prod",
	// 	cfg.Resources.gatewayID))

	// _, err = l.AddPermission(permissionsInput)

	// if err != nil {
	// 	fmt.Println(*permissionsInput.SourceArn)
	// 	fmt.Println(*permissionsInput.StatementId)
	// 	fmt.Println(err)
	// 	return err
	// }

	_, err = g.PutMethod(&apigateway.PutMethodInput{
		HttpMethod:        method,
		RestApiId:         aws.String(cfg.Resources.gatewayID),
		ResourceId:        rootResource.Id,
		AuthorizationType: aws.String("AWS_IAM"),
	})

	if err != nil {
		return err
	}

	uriString := fmt.Sprintf("arn:aws:apigateway:%s:lambda:path/2015-03-31/functions/%s/invocations",
		aws.StringValue(regionConfig.Region),
		aws.StringValue(functionARN))

	params := &apigateway.PutIntegrationInput{
		HttpMethod:            method,
		ResourceId:            rootResource.Id,
		RestApiId:             gw.Id,
		Type:                  aws.String("AWS_PROXY"),
		IntegrationHttpMethod: method,
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
		HttpMethod:       method,
		ResourceId:       rootResource.Id,
		RestApiId:        aws.String(cfg.Resources.gatewayID),
		StatusCode:       aws.String("200"),
		SelectionPattern: aws.String(".*"),
	}
	_, err = g.PutIntegrationResponse(integrationResponseParams)

	if err != nil {
		return err
	}

	methodResponsParams := &apigateway.PutMethodResponseInput{
		HttpMethod:     method,
		ResourceId:     rootResource.Id,
		RestApiId:      aws.String(cfg.Resources.gatewayID),
		StatusCode:     aws.String("200"),
		ResponseModels: map[string]*string{},
	}
	_, err = g.PutMethodResponse(methodResponsParams)
	return err
}

func (scfg *AWSConfig) EnsureRole(roleName string, policyDocument string) (arn string) {
	i := iam.New(scfg.awsSession)
	r, err := i.CreateRole(&iam.CreateRoleInput{
		RoleName:                 aws.String(roleName),
		AssumeRolePolicyDocument: aws.String(policyDocument),
	})

	// Role successfully created
	if err == nil {
		return *r.Role.Arn
	}

	// Get existing Role
	xr, err := i.GetRole(&iam.GetRoleInput{
		RoleName: aws.String(roleName),
	})

	if err != nil {
		log.Fatalf("Not able to ensure role %s", roleName)
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
	sess, err := session.NewSession(&aws.Config{
		Credentials: credentials.NewEnvCredentials(),
	})
	g := apigateway.New(sess, &aws.Config{Region: &scfg.region})

	if err != nil {
		return err
	}

	i := iam.New(sess)
	scfg.EnsureRole(scfg.Resources.gatewayRoleName,
		fmt.Sprintf(ExternalApiRoleTrustDocument,
			scfg.iamUserARN,
			scfg.apiExternalID))

	if err != nil {
		return err
	}

	pd := fmt.Sprintf(InvokeExternalApiPolicyDocument,
		scfg.region,
		scfg.awsAccount,
		scfg.Resources.gatewayID)

	putParams := &iam.PutRolePolicyInput{
		PolicyDocument: aws.String(pd),
		PolicyName:     aws.String(scfg.Resources.gatewayPolicyName),
		RoleName:       aws.String(scfg.Resources.gatewayRoleName),
	}
	_, err = i.PutRolePolicy(putParams)

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

	scfg.executeSnowflakeQuery(fmt.Sprintf(`create or replace external function jqa_test_external_func(n integer, v varchar)
    returns variant
    api_integration = jqa_test_api_integration
    as '%s'
    ;`, scfg.Resources.gatewayEndpoint), func(scan func(dest ...interface{}) error) error {
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
