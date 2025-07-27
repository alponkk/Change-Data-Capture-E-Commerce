// MongoDB Replica Set Initialization Script
// This script initializes a single-node replica set named "rs0"

print("Starting replica set initialization...");

try {
  // Check if replica set is already initialized
  var status = rs.status();
  print("Replica set is already initialized:");
  print(JSON.stringify(status, null, 2));
} catch (err) {
  // Replica set not initialized, proceed with initialization
  print("Replica set not initialized. Initializing now...");
  
  var config = {
    _id: "rs0",
    members: [
      { _id: 0, host: "mongo:27017" }
    ]
  };
  
  try {
    var result = rs.initiate(config);
    print("Replica set initialization result:");
    print(JSON.stringify(result, null, 2));
    
    if (result.ok === 1) {
      print("✅ Replica set 'rs0' initialized successfully!");
      
      // Wait a moment for the replica set to stabilize
      print("Waiting for replica set to stabilize...");
      sleep(3000);
      
      // Check the status
      var finalStatus = rs.status();
      print("Final replica set status:");
      print("State: " + finalStatus.myState);
      print("Members:");
      finalStatus.members.forEach(function(member) {
        print("  - " + member.name + " (state: " + member.stateStr + ")");
      });
    } else {
      print("❌ Failed to initialize replica set");
      print("Error details:", result);
    }
  } catch (initError) {
    print("❌ Error during replica set initialization:");
    print(initError.toString());
  }
}

print("Script execution completed."); 