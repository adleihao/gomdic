package main 

import (
	"encoding/json"
	"reflect"
	"time"
	"strconv"
	"strings"
	"fmt"
	"github.com/adleihao/gomdic"
	"code.google.com/p/log4go"

)

type Tdic struct{
	name string
	age int
}
func (t *Tdic)ParseLine(line string)(key interface{}, value interface{}){
	line = strings.Trim(line," ")
	line = strings.Trim(line,"\t")
	line_arr := strings.Split(line,"\t")
	if len(line_arr) != 2 {
		return 
	}
	key ,_= strconv.Atoi(line_arr[0])
	v_list := strings.Split(line_arr[1], ",")
	v_arr :=make([]int,len(v_list))
	for idx,v := range v_list {
		v_arr[idx],_ = strconv.Atoi(v)
	}
	
	value = v_arr
	fmt.Println("name ", t.name, " age ", t.age)
	return
}

type UserProfile struct {
	Name string `json:"Name"`
	Age int `json:"Age"`
	Interest []int `json:Interest`
}
func (up *UserProfile)ParseLine(line string)(key interface{}, value interface{}){
	line = strings.Trim(line," ")
	line = strings.Trim(line,"\t")
	line_arr := strings.Split(line,"\t")
	if len(line_arr) != 2 {
		return 
	}
	key = line_arr[0]
	v_prt := &UserProfile{}
	json.Unmarshal([]byte(line_arr[1]), v_prt)
	value = v_prt
	return 
}

func main(){
	log4sys := make(log4go.Logger)
	log4sys.LoadConfiguration("./conf/syslog.xml")


	gomdic.InitMdic(log4sys, 3*time.Second)
	sMdic := gomdic.GetMdic()
	tdic := Tdic{name:"leihao", age :28}
	
	//t
	sMdic.Register("t1","./data/t1.data",&tdic)
	sMdic.UpdateData()
	
	val, err := sMdic.GetValueByKeys("t1", 1)
	fmt.Println(reflect.TypeOf(val))
	fmt.Println(val, err)
	
	//UserProfile value is  Struct 
	userProfile := UserProfile{}
	upStr := "leihao" + "\t" + `{"Name":"leihao"}`
	fmt.Println("upStr ", upStr)
	k,v := userProfile.ParseLine(upStr)
	fmt.Println("userProfile k,v : ",k,v )
	
	sMdic.Register("UserProfile", "./data/t2.data", &userProfile)
	v , err = sMdic.GetValueByKeys("UserProfile","leihao","xitao","fangfang")
	vP := v.([]interface{})
	v2 := vP[0].(*UserProfile)
	fmt.Println("UserProfile 通过Mdic获得 ", v2)
	
	fmt.Println("-------- end ----")
	a := make(chan int)
	select{
		case <-a:
		case <-time.After(1000 * time.Second):
			fmt.Println("time.After")

	}
	
}