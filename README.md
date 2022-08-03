NTU FYP project by Johnson Wong Teng Fung 

Proj No:	        C177

Title:	            Study of Wireless Sensor Hardware for Internet of Things Facility Management (IoT-FM) Environment Sensing

Main Supervisor:	Asst Prof Li King Ho, Holden

# Data ingestion from Particle Cloud -> Google Pub/Sub -> Google Firestore Database

This Javascript program utilizes Node.js to listen for messages from Google Pub/Sub whereby data from Particle Cloud enters. It will take the blob of sensor data readings sets and format it into readable data, to be stored into a database that is Google Firestore.

Resources followed include https://cloud.google.com/pubsub/docs/publish-receive-messages-client-library , https://firebase.google.com/docs/firestore/manage-data/add-data , and https://github.com/particle-iot/google-cloud-datastore-tutorial