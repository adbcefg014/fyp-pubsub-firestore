const {PubSub} = require('@google-cloud/pubsub');
const { initializeApp, applicationDefault, cert } = require('firebase-admin/app');
const { getFirestore, Timestamp, FieldValue } = require('firebase-admin/firestore');

/* CONFIGURATION */
let config = {
    gcpProjectId: 'c177-sensors',
    gcpPubSubSubscriptionName: 'projects/c177-sensors/subscriptions/ingest',
    gcpServiceAccountKeyFilePath: './gcp_private_key.json'
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
    
        storeEvent(message, inObj);
        message.ack();
    };

    // Listen for new messages until timeout is hit
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




async function storeEvent(message, inObj) {
    let dataArrayGroupSet;

    // Process incoming data
    try {
        dataArrayGroupSet = JSON.parse(String(message.data));
    } catch (err) {
        // Incoming data is not in format of the sensors' data, thus enter into Firestore as-is
        const storedData = await db.collection(message.attributes.device_id).add(inObj);
        console.log("Particle event stored in Firestore!\r\n")
        return;
    }
    
    // Process the incoming sensor data set(s) for Database insertion
    for (let dataArraySet = 0; dataArraySet < dataArrayGroupSet.length; dataArraySet++) {
        // Add each array element of the sensor data set into obj, corresponding to its representation
        let dataObj = {};
        let reading;
        for (let dataReading = 0; dataReading < dataArrayGroupSet[dataArraySet].length; dataReading++) {
            switch (dataReading) {
                case 0:
                    reading = 'Timestamp';
                    break;
                case 1:
                    reading = 'Light level (lux)';
                    break;
                case 2:
                    reading = 'Loudness (dB)';
                    break;
                case 3:
                    reading = 'UV light level';
                    break;
                case 4:
                    reading = 'Pressure (mBar)';
                    break;
                case 5:
                    reading = 'Temperature (*C)';
                    break;
                case 6:
                    reading = 'Relative Humidity (%)';
                    break;
                case 7:
                    reading = 'PM1.0 (μg/m3)';
                    break;
                case 8:
                    reading = 'PM2.5 (μg/m3)';
                    break; 
                case 9:
                    reading = 'PM4.0 (μg/m3)';
                    break;
                case 10:
                    reading = 'PM10.0 (μg/m3)';
                    break;
                    case 11:
                        reading = 'CO2 (ppm)';
                        break;
            }
            dataObj[reading] = dataArrayGroupSet[dataArraySet][dataReading];
        }

        // Add processed set of sensor readings to Database
        const storedData = await db.collection(message.attributes.device_id).add(dataObj);
        console.log("Sensor reading set stored in Firestore\r\n");
        console.log(dataObj);
    }
};
/* END FIRESTORE */