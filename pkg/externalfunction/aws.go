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
	gatewayDeploymentID    string
	gatewayStage           string
	lambdaFunctionZipBytes []byte
	regionConfig           *aws.Config
}

const (
	LambdaZip     = `aW1wb3J0IGpzb24KCmRlZiBsYW1iZGFfaGFuZGxlcihldmVudCwgY29udGV4dCk6CgogICAgIyAyMDAgaXMgdGhlIEhUVFAgc3RhdHVzIGNvZGUgZm9yICJvayIuCiAgICBzdGF0dXNfY29kZSA9IDIwMAoKICAgICMgVGhlIHJldHVybiB2YWx1ZSB3aWxsIGNvbnRhaW4gYW4gYXJyYXkgb2YgYXJyYXlzIChvbmUgaW5uZXIgYXJyYXkgcGVyIGlucHV0IHJvdykuCiAgICBhcnJheV9vZl9yb3dzX3RvX3JldHVybiA9IFsgXQoKICAgIHRyeToKICAgICAgICAjIEZyb20gdGhlIGlucHV0IHBhcmFtZXRlciBuYW1lZCAiZXZlbnQiLCBnZXQgdGhlIGJvZHksIHdoaWNoIGNvbnRhaW5zCiAgICAgICAgIyB0aGUgaW5wdXQgcm93cy4KICAgICAgICBldmVudF9ib2R5ID0gZXZlbnRbImJvZHkiXQoKICAgICAgICAjIENvbnZlcnQgdGhlIGlucHV0IGZyb20gYSBKU09OIHN0cmluZyBpbnRvIGEgSlNPTiBvYmplY3QuCiAgICAgICAgcGF5bG9hZCA9IGpzb24ubG9hZHMoZXZlbnRfYm9keSkKICAgICAgICAjIFRoaXMgaXMgYmFzaWNhbGx5IGFuIGFycmF5IG9mIGFycmF5cy4gVGhlIGlubmVyIGFycmF5IGNvbnRhaW5zIHRoZQogICAgICAgICMgcm93IG51bWJlciwgYW5kIGEgdmFsdWUgZm9yIGVhY2ggcGFyYW1ldGVyIHBhc3NlZCB0byB0aGUgZnVuY3Rpb24uCiAgICAgICAgcm93cyA9IHBheWxvYWRbImRhdGEiXQoKICAgICAgICAjIEZvciBlYWNoIGlucHV0IHJvdyBpbiB0aGUgSlNPTiBvYmplY3QuLi4KICAgICAgICBmb3Igcm93IGluIHJvd3M6CiAgICAgICAgICAgIyBSZWFkIHRoZSBpbnB1dCByb3cgbnVtYmVyICh0aGUgb3V0cHV0IHJvdyBudW1iZXIgd2lsbCBiZSB0aGUgc2FtZSkuCiAgICAgICAgICAgIHJvd19udW1iZXIgPSByb3dbMF0KICAgICAgICAKCiAgICAgICAgICAgICMgQ29tcG9zZSB0aGUgb3V0cHV0IGJhc2VkIG9uIHRoZSBpbnB1dC4gVGhpcyBzaW1wbGUgZXhhbXBsZQogICAgICAgICAgICAjIG1lcmVseSBlY2hvZXMgdGhlIGlucHV0IGJ5IGNvbGxlY3RpbmcgdGhlIHZhbHVlcyBpbnRvIGFuIGFycmF5IHRoYXQKICAgICAgICAgICAgIyB3aWxsIGJlIHRyZWF0ZWQgYXMgYSBzaW5nbGUgVkFSSUFOVCB2YWx1ZS4KICAgICAgICAgICAgb3V0cHV0X3ZhbHVlID0gWyJFY2hvaW5nIGlucHV0czoiXQogICAgICAgICAgICAKICAgICAgICAgICAgZm9yIGkgaW4gcmFuZ2UobGVuKHJvd1sxOl0pKToKICAgICAgICAgICAgICAgIG91dHB1dF92YWx1ZS5hcHBlbmQocm93W2ldKQoKICAgICAgICAgICAgIyBQdXQgdGhlIHJldHVybmVkIHJvdyBudW1iZXIgYW5kIHRoZSByZXR1cm5lZCB2YWx1ZSBpbnRvIGFuIGFycmF5LgogICAgICAgICAgICByb3dfdG9fcmV0dXJuID0gW3Jvd19udW1iZXIsIG91dHB1dF92YWx1ZV0KCiAgICAgICAgICAgICMgLi4uIGFuZCBhZGQgdGhhdCBhcnJheSB0byB0aGUgbWFpbiBhcnJheS4KICAgICAgICAgICAgYXJyYXlfb2Zfcm93c190b19yZXR1cm4uYXBwZW5kKHJvd190b19yZXR1cm4pCgogICAgICAgIGpzb25fY29tcGF0aWJsZV9zdHJpbmdfdG9fcmV0dXJuID0ganNvbi5kdW1wcyh7ImRhdGEiIDogYXJyYXlfb2Zfcm93c190b19yZXR1cm59KQoKICAgIGV4Y2VwdCBFeGNlcHRpb24gYXMgZXJyOgogICAgICAgICMgNDAwIGltcGxpZXMgc29tZSB0eXBlIG9mIGVycm9yLgogICAgICAgIHN0YXR1c19jb2RlID0gNDAwCiAgICAgICAgIyBUZWxsIGNhbGxlciB3aGF0IHRoaXMgZnVuY3Rpb24gY291bGQgbm90IGhhbmRsZS4KICAgICAgICBqc29uX2NvbXBhdGlibGVfc3RyaW5nX3RvX3JldHVybiA9IGV2ZW50X2JvZHkKCiAgICAjIFJldHVybiB0aGUgcmV0dXJuIHZhbHVlIGFuZCBIVFRQIHN0YXR1cyBjb2RlLgogICAgcmV0dXJuIHsKICAgICAgICAnc3RhdHVzQ29kZSc6IHN0YXR1c19jb2RlLAogICAgICAgICdib2R5JzoganNvbl9jb21wYXRpYmxlX3N0cmluZ190b19yZXR1cm4KICAgIH0K`
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
	}

	return cfg, nil
}

func APIARN(apiID *string, functionARN *string, functionName *string) string {
	apiArn := strings.Replace(aws.StringValue(functionARN), "lambda", "execute-api", 1)
	return strings.Replace(apiArn,
		fmt.Sprintf("function:%s", aws.StringValue(functionName)),
		aws.StringValue(apiID), 1)
}

func (cfg *AWSConfig) ConfigureAwsRoles() error {

	a := iam.New(cfg.awsSession)

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

	s := sts.New(cfg.awsSession)
	id, err := s.GetCallerIdentity(&sts.GetCallerIdentityInput{})

	cfg.awsAccount = *id.Account

	lrole, err := a.GetRole(&iam.GetRoleInput{
		RoleName: aws.String(cfg.Resources.lambdaRoleName),
	})

	cfg.Resources.lambdaRoleARN = *lrole.Role.Arn
	l := lambda.New(cfg.awsSession, cfg.Resources.regionConfig)

	var functionARN *string
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

	g := apigateway.New(cfg.awsSession, cfg.Resources.regionConfig)

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
		cfg.region,
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
