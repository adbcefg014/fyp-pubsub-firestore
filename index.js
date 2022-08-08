const {PubSub} = require('@google-cloud/pubsub');
const { initializeApp, applicationDefault, cert } = require('firebase-admin/app');
const { getFirestore, Timestamp, FieldValue } = require('firebase-admin/firestore');
const Particle = require('particle-api-js');

/* CONFIGURATION */
let config = {
    gcpProjectId: 'c177-sensors',
    gcpPubSubSubscriptionName: 'projects/c177-sensors/subscriptions/ingest',
    gcpServiceAccountKeyFilePath: './gcp_private_key.json',
    particleAccountLoginFilePath: './particle_login.json'
};
/* END CONFIGURATION */

/* PUBSUB */
console.log('Authenticating PubSub with Google Cloud...');
const pubSubClient = new PubSub({
    projectId: config.gcpProjectId,
    keyFilename: config.gcpServiceAccountKeyFilePath,
})
console.log('Authentication successful!');

function listenForMessages() {
    // References an existing subscription
    const subscription = pubSubClient.subscription(config.gcpPubSubSubscriptionName);
    
    // Create an event handler to handle messages
    let inObj = {};
    const messageHandler = message => {
        console.log("Received event from Pub/Sub\r\n");
        inObj = {
            gc_pub_sub_id: message.id,
            device_id: message.attributes.device_id,
            event: message.attributes.event,
            data: String(message.data),
            published_at: message.attributes.published_at
        };
        console.log(inObj);
        console.log("");
    
        processIncomingEvent(inObj);
        message.ack();
    };

    // Listen for new messages
    subscription.on('message', messageHandler);
}

listenForMessages();
/* END PUBSUB */


/* FIRESTORE */
console.log('Authenticating Firestore with Google Cloud...');
initializeApp({
    credential: cert(config.gcpServiceAccountKeyFilePath)
});
const db = getFirestore();
console.log("Authentication successful!");


async function processIncomingEvent(inObj) {
    // Check if any interval update for device that came online
    if (inObj.event === "spark/status") {
        if (inObj.data === "online") checkUpdate(inObj.device_id);
        return;
    }

    // Process incoming data from "sensor-readings" event
    let dataArrayGroupSet;
    try {
        dataArrayGroupSet = JSON.parse(inObj.data);
    } catch (err) {
        // Incoming data is not in format of the sensors' data, thus enter into Firestore as-is
        const storedData = await db.collection(inObj.device_id).add(inObj);
        console.log("Particle event stored in Firestore!\r\n")
        return;
    }
    
    // Process the incoming sensor data set(s) for Database insertion
    for (let dataArraySet = 0; dataArraySet < dataArrayGroupSet.length; dataArraySet++) {
        // Add each array element of the sensor data set into obj, corresponding to its representation
        let dataObj = {};
        let reading;
        for (let dataReading = 0; dataReading < dataArrayGroupSet[dataArraySet].length; dataReading++) {
            /*
            0:  Timestamp
            1:  Light level (lux)
            2:  Loudness (dB)
            3:  UV light level
            4:  Pressure (mBar)
            5:  Temperature (*C)
            6:  Relative Humidity (%)
            7:  PM1.0 (μg/m3)
            8:  PM2.5 (μg/m3)
            9:  PM4.0 (μg/m3)
            10: PM10.0 (μg/m3)
            11: CO2 (ppm)
            */
            dataObj[dataReading] = dataArrayGroupSet[dataArraySet][dataReading];
        }

        // Add processed set of sensor readings to Database
        const storedData = await db.collection(inObj.device_id).add(dataObj);
        console.log("Sensor reading set stored in Firestore\r\n");
        console.log(dataObj);
    }
};
/* END FIRESTORE */


/* PARTICLE */
console.log ("Authenticating with Particle Cloud...")
let particleLogin = require(config.particleAccountLoginFilePath);
let particle = new Particle();
let particleToken;
particle.login({username: particleLogin.username, password: particleLogin.password}).then(
    function(data) {
      particleToken = data.body.access_token;
    },
    function (err) {
      console.log('Could not log in.', err);
    }
  );
console.log("Authentication successful!");

/* 
*   For Firestore collection "updates"
*   Document name -> device_id the update is pending for
*   Each document should only contain 1 or 2 fields:
*       a : newSensingInterval          // default/minimum 120000 (ms), compulsory
*       b : newIntervalCompensation     // default/minimum 0, optional
*       c : readingsToCollate           // default/minimum 1, optional
*/  
let pendingUpdatesBool = false;
let pendingUpdates = {};
const pendingUpdatesFirestore = db.collection("updates");
const updatedIntervalsFirestore = db.collection("updated-intervals")
const observer = pendingUpdatesFirestore.onSnapshot(querySnapshot => {
    querySnapshot.docChanges().forEach(change => {
        if (change.type === 'added' || change.type === 'modified') {
            let updateArray = [];
            ({a, b, c} = change.doc.data());
            if (!Number.isInteger(a) || a < 120000 || b < 0 || c < 1) { 
                console.log("invalid update received");
                pendingUpdatesFirestore.doc(change.doc.id).delete();
                return;
            }
            updateArray[0] = a;
            updateArray[1] = 0;
            updateArray[2] = 1;
            if (b && Number.isInteger(b)) updateArray[1] = b;
            if (c && Number.isInteger(c)) updateArray[2] = c;
            
            pendingUpdatesBool = true;
            pendingUpdates[change.doc.id] = updateArray;
            console.log('Updated pending updates: ', pendingUpdates);
            console.log('Pending updates', pendingUpdatesBool);
        }
        if (change.type === 'removed') {
            delete pendingUpdates[change.doc.id];
            if (Object.keys(pendingUpdates).length === 0) pendingUpdatesBool = false;
            console.log('Updated pending updates: ',pendingUpdates);
            console.log('Pending updates', pendingUpdatesBool);
        }
    })
}, err => {
    console.log(`Encountered error: ${err}`);
});
/* END PARTICLE*/


/* HELPERS*/
function checkUpdate(device_id) {
    if (!pendingUpdatesBool) return;
    if (pendingUpdates.hasOwnProperty(device_id)) {
        updateIntervals(device_id);
    }
}

function updateIntervals(device_id) {
    let updateArrayString = "[" + pendingUpdates[device_id].toString() + "]";

    let functionParticle = particle.callFunction({
        deviceId: device_id,
        name: "adjustIntervals",
        argument: updateArrayString,
        auth: particleToken
    });
    
    functionParticle.then(
        function(data) {
            console.log('Function called succesfully:', data);

            // Record new intervals to Firestore
            updatedIntervalsFirestore.doc(change.doc.id).set({
                a: pendingUpdates[device_id][0],
                b: pendingUpdates[device_id][1],
                c: pendingUpdates[device_id][2]
            });
            pendingUpdatesFirestore.doc(device_id).delete();
        }, function(err) {
            console.log('An error occurred:', err);
        });
}
/* END HELPERS*/