var express = require('express')
var app = express()
var AWS = require("aws-sdk");
var fs = require('fs');
var csv = require('fast-csv');

//Define request response in root URL (/)
app.get('/', function (req, res) {
    res.send('Hello World!')
  
    var kinesis = new AWS.Kinesis({
      accessKeyId: "TBD",
      secretAccessKey: "TBD",
      region: "TBD"
      });
    if(!kinesis){
    //warning to connect with AWS 
    return;
    }
    else
    {
      console.log("AWS Connected!!")
    }

    var shardNumber=1;
    var records = []; // we would like to send record in an array, so that we can send bulk data to Kinesis
    var maxNumberOfShards =500;
    var streamName = "myStream";

    const log = require('simple-node-logger').createSimpleFileLogger('project.log');
    var stream = fs.createReadStream('./Data/lightTraffic.csv');
    csv.fromStream(stream, {headers : true}).on("data", function(data){
        var partitionKey = 'TestKey'+shardNumber.toString();
        var record = {Data: JSON.stringify(data),PartitionKey: partitionKey};
        console.log(record);

        shardNumber++; //each csv row is directed to a new shared which repetes it self once we fill maxNumberOfShards
        records.push(record);
        if(records.length == maxNumberOfShards){
            var recordsParams = {
              Records: records,
              StreamName: streamName
            };
          console.log("About to add record to Kinesis ")  
          kinesis.putRecords(recordsParams, function(err, data) {
            if(err){
              log.info('Error while uploading records', err);
            }
            else{
              console.log("Data has been archived to Kinesis")
              if(data.FailedRecordCount > 0){
                //TBD: need to handle failed records.
              }
            }
            
          })
        records=[];
        shardNumber=1;
        }
        
      
    })  // csv close

	
	
})//close of entry funtion 

//Launch listening server on port 8081
app.listen(8081, function () {
  console.log('app listening on port 8081!')
})




