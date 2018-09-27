var express = require('express')
var app = express()
var AWS = require("aws-sdk");
var fs = require('fs');
var csv = require('fast-csv');
const log = require('simple-node-logger').createSimpleFileLogger('project.log');

//Launch listening server on port 8081
app.listen(8081, function () {
  console.log('app listening on port 8081!')
})


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
    var maxNumberOfShards =10;
    var streamName = "myStream";
    var batchcount=0;

    
    var stream = fs.createReadStream('./Data/lightTraffic.csv');
    csv.fromStream(stream, {headers : true}).on("data", function(data){
        var partitionKey = 'TestKey'+shardNumber.toString();
        var record = {Data: JSON.stringify(data),PartitionKey: partitionKey};
         //console.log(record);
        shardNumber++; //each csv row is directed to a new shared which repetes it self once we fill maxNumberOfShards
        records.push(record);
        if(records.length == maxNumberOfShards){
          batchcount++;
          ArchiveDataToKinesis(records,streamName,kinesis);
          records=[];
          shardNumber=1;
        }       
      
    }) .on("end", function(){
      console.log('total batch',batchcount);    
      console.log("Data has been archived to Kinesis");
  });  // csv close

	
	
})//close of entry funtion 


function ArchiveDataToKinesis(records,streamName,kinesis ){
  var recordsParams = {
    Records: records,
    StreamName: streamName
  };
  console.log("About to add record to Kinesis ")
  kinesis.putRecords(recordsParams, function(err, data) {
    var temprecordsParams = recordsParams;
    if(err){
      log.info('Error while uploading records', err);
    }
    else{
       console.log("Data has been archived to Kinesis")
       if(data.FailedRecordCount > 0){
          console.log('Unable to upload complete record .. we have  %d failures.', data.FailedRecordCount);
          log.info('Unable to upload complete record .. we have  %d failures.', data.FailedRecordCount);
          failedRecords= [];
          data.Records.forEach(record,index => {
          if(record.ErrorCode){
            failedRecords.push(temprecordsParams.Records[index]);
          }
        });
        if(failedRecords.length>0){
        console.log("Adding Failed Record to Stream again ",failedRecords);  
        log.info("Adding Failed Record to Stream again ",failedRecords);
        ArchiveDataToKinesis(failedRecords,streamName,kinesis);       

        }
      }

    }
    
  })  
}




