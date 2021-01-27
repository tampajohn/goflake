package externalfunction

import (
	"fmt"
	"log"
	"strings"

	"github.com/manifoldco/promptui"
	"github.com/tampajohn/goflake/pkg/common"
)

var (
	wscol = 30
	wsRow = 30
)

type provider int

const (
	AWS provider = iota
)

func (o provider) String() string {
	supported := [...]string{"AWS"}
	if int(o) > len(supported)-1 {
		return common.NOTSUPPORTED
	}
	return supported[o]
}

func Start() {
	prompt := promptui.Select{
		Label: "What cloud provider would you like ?",
		Items: []provider{AWS},
	}
	idx, _, err := prompt.Run()

	if err != nil {
		log.Fatalf("Prompt failed %v\n", err)
		return
	}
	selected := provider(idx)
	switch selected {
	case AWS:
		funcSig := common.PromptStringWithValidator(
			"What is the function's signature?",
			false,
			"external_func(n int, v varchar)",
			func(s string) error {
				if len(s) == 0 || strings.Index(s, "(") < 1 || !strings.HasSuffix(s, ")") {
					return fmt.Errorf("'%s' is not a valid function signature.", s)
				}
				return nil
			})
		funcSig = strings.TrimSpace(funcSig)
		fn := strings.Split(funcSig, "(")[0]

		cfg, err := NewAWSConfig(fn, funcSig)
		if err != nil {
			log.Fatalf("Error encountered: %s\n", err)
		}
		err = cfg.ConfigureAwsRoles()
		if err != nil {
			log.Fatalf("Error encountered: %s\n", err)
		}

		scfg := NewSnowflakeConfig(cfg)
		err = scfg.AddTrustToAWSRole()
		if err != nil {
			log.Fatalf("Error encountered: %s\n", err)
		}
	default:
		log.Fatalf("%s is not supported at this time.\n", selected)
	}
}
