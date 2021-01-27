package common

import (
	"fmt"
	"log"
	"os"

	"github.com/buger/goterm"
	"github.com/manifoldco/promptui"
)

// NOTSUPPORTED is used to represent unsupported options
const NOTSUPPORTED = "NOT_SUPPORTED"

func AskYesNo(question string) bool {
	prompt := promptui.Select{
		Label: question,
		Items: []string{"Yes", "No"},
	}
	_, result, err := prompt.Run()
	if err != nil {
		log.Fatalf("Prompt failed %v\n", err)
	}
	return result == "Yes"
}

func AskOptions(question string, options []string) (int, string) {
	prompt := promptui.Select{
		Label: question,
		Items: options,
	}
	idx, result, err := prompt.Run()
	if err != nil {
		log.Fatalf("Prompt failed %v\n", err)
	}
	return idx, result
}

func EnvOrString(envVariable string, mask bool) string {
	if value, isFound := os.LookupEnv(envVariable); isFound {
		return value
	}
	goterm.Printf("No %s was found.\n", envVariable)
	goterm.Flush()
	return PromptString(envVariable, mask, "")
}

func PromptString(question string, mask bool, defValue string) string {
	prompt := promptui.Prompt{
		Label:   question,
		Default: defValue,
		Validate: func(s string) error {
			if len(s) > 0 {
				return nil
			}
			return fmt.Errorf("Please provide an answer")
		},
	}
	if mask {
		prompt.Mask = '*'
	}
	result, err := prompt.Run()
	if err != nil {
		log.Fatalf("Prompt failed %v\n", err)
	}
	return result
}

func PromptStringWithValidator(question string, mask bool, defValue string, validator func(string) error) string {
	prompt := promptui.Prompt{
		Label:    question,
		Default:  defValue,
		Validate: validator,
	}
	if mask {
		prompt.Mask = '*'
	}
	result, err := prompt.Run()
	if err != nil {
		log.Fatalf("Prompt failed %v\n", err)
	}
	return result
}
