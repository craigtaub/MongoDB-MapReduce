<?php
try {

//export data from live mongo (master) (db->collection)
$export = "mongoexport --host apiMongoSet/IP:27017 --slaveOk 1 --collection collection --db db -o /home/scripts/mongoexport_all.json";
system($export);
echo "Export master \n";

//import here
$import = "mongoimport --collection collection --db db --file /home/scripts/mongoexport_all.json";
system($import);
echo "Import here \n";

$delete = "rm /home/scripts/mongoexport_all.json";
system($delete);
echo "Delete \n";

//run MapReduce

      $conn = new Mongo('mongodb://localhost:27017');
      MongoCursor::$timeout = -1;

      $db = $conn->db;

      $map = new MongoCode("function() {
        emit({username: this.user.username, user_id: this._id}, {count: 1});
      }");

      $reduce = new MongoCode("function(key, values) {
        var count = 0;
        values.forEach(function(v) {
          count += v['count'];
        });
        return {count: count};
      }
      ");

      $mapTwo = new MongoCode("function() {
        emit(this['_id']['username'], {count: 1});
      }");


     $db->command(array(
          "mapreduce" => "data", 
          "map" => $map,
          "reduce" => $reduce,
          "out" => "data_results",
          'query'     => array( "user" => array( '$exists' => true, ) )));

      $db->command(array(
          "mapreduce" => "data_results", 
          "map" => $mapTwo,
          "reduce" => $reduce,
          "out" => "data_results_unique"));
echo "MapReduce run \n";



//export data_results_unique
$exportTwo = "mongoexport --collection data_results_unique --db  db -o /home/scripts/mongoexport_sys.json";
system($exportTwo);
echo "Self export \n";

//import into live mongo server (master)
$importTwo = "mongoimport --host apiMongoSet/IP:27017 --collection data_results_unique --db db --file /home/scripts/mongoexport_sys.json --upsert";
system($importTwo);
echo "Imported into live mongo \n";

//delete
$deleteTwo = "rm /home/scripts/mongoexport_sys.json";
system($deleteTwo);
echo "Delete \n";

echo "DONE";

} catch (Exception $e) { 
var_dump($e);

die('dead');

}

