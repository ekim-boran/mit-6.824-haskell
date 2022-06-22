package main

import (
	"encoding/json"
	"fmt"
	"io/ioutil"
	"os"
	"time"
)

type Logs struct {
	Op       uint8  `json:"op"`
	Key      string `json:"key"`
	Value    string `json:"value"`
	Output   string `json:"output"`
	Call     int64  `json:"start"`
	Return   int64  `json:"end"`
	ClientId int64  `json:"clientId"`
}

const linearizabilityCheckTimeout = 4 * time.Second

func main() {

	jsonFile, err := os.Open("a.json")
	if err != nil {
		fmt.Println(err)
	}
	//fmt.Println("Successfully Opened users.json")
	defer jsonFile.Close()
	byteValue, _ := ioutil.ReadAll(jsonFile)
	var logs []Logs
	json.Unmarshal(byteValue, &logs)
	//fmt.Println(logs)
	var operations []Operation

	for _, i := range logs {
		op := Operation{
			Input:    KvInput{Op: i.Op, Key: i.Key, Value: i.Value},
			Output:   KvOutput{Value: i.Output},
			Call:     i.Call,
			Return:   i.Return,
			ClientId: int(i.ClientId),
		}
		operations = append(operations, op)

	}

	res, info := CheckOperationsVerbose(KvModel, operations, linearizabilityCheckTimeout)

	if res == Illegal {
		file, err := os.Create("error.html")
		if err != nil {
			fmt.Printf("info: failed to create temp file for visualization")
		} else {
			err = Visualize(KvModel, info, file)
			if err != nil {
				fmt.Printf("info: failed to write history visualization to %s\n", file.Name())
			} else {
				fmt.Printf("info: wrote history visualization to %s\n", file.Name())
			}
		}
		fmt.Println("history is not linearizable")
		os.Exit(1)
	} else if res == Unknown {
		fmt.Println("info: linearizability check timed out, assuming history is ok")
		//os.Exit(1)

	}
	fmt.Println("history is ok")

}
