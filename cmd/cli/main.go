package main

import (
	"fmt"
	"log"

	"github.com/buger/goterm"
	"github.com/manifoldco/promptui"
	"github.com/tampajohn/goflake/pkg/common"
	"github.com/tampajohn/goflake/pkg/externalfunction"
)

type topOption int

const banner = `
 _______  _______  _______  ___      _______  ___   _  _______ 
|       ||       ||       ||   |    |   _   ||   | | ||       |
|    ___||   _   ||    ___||   |    |  |_|  ||   |_| ||    ___|
|   | __ |  | |  ||   |___ |   |    |       ||      _||   |___ 
|   ||  ||  |_|  ||    ___||   |___ |       ||     |_ |    ___|
|   |_| ||       ||   |    |       ||   _   ||    _  ||   |___ 
|_______||_______||___|    |_______||__| |__||___| |_||_______|
===============================================================
---------------------------- beta -----------------------------


`

const (
	// ExternalFunction allows the creation of a Snowflake Function that calls a Cloud Proxy endpoint
	ExternalFunction topOption = iota
	// SSOIntegration allows the creation of an SSO Integration in Snowflake
	SSOIntegration
	// Deletes all API Gateways
	DeleteAllGateways
)

func (o topOption) String() string {
	supported := [...]string{"External Function", "SSO Integration", "Delete All Gateways"}
	if int(o) > len(supported)-1 {
		return common.NOTSUPPORTED
	}
	return supported[o]
}

func main() {
	goterm.Clear()
	goterm.Flush()
	goterm.MoveCursor(1, 1)
	goterm.Flush()
	fmt.Print(banner)

	prompt := promptui.Select{
		Label: "What do you want make?",
		Items: []topOption{ExternalFunction, SSOIntegration},
	}

	idx, _, err := prompt.Run()

	if err != nil {
		log.Fatalf("Prompt failed %v\n", err)
		return
	}
	selected := topOption(idx)
	switch selected {
	case ExternalFunction:
		externalfunction.Start()
	default:
		log.Fatalf("%s is not supported at this time.\n", selected)
	}
}
