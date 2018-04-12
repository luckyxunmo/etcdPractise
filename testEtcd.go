package main

import(
 "practise/etcdclient"
 "fmt"
)
func main(){
first()
// election.ExampleElection_Campaign()
}
func first(){
  client,err := etcdclient.New("http://127.0.0.1:2379","",0)
  if err != nil{
    fmt.Println("creat client err, err is ",err)
  }
  client.Create("/test",[]byte("123"))
  data,err :=client.Read("/test",false)
  fmt.Println("data is ",string(data))
 err = client.Create("/test",[]byte("4565"))
  if err != nil{
    fmt.Println("creat client err, err is ",err)
  }

  data,err =client.Read("/test",false)
  fmt.Println("data is ",string(data))
 
  client.Close()
}
