package externalfunction

import (
	"log"

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
		fn := common.PromptString("What would you like the externalFunc to be called?", false)
		cfg, err := NewAWSConfig(fn)
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
