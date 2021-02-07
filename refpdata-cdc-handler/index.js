const Firestore = require('@google-cloud/firestore');
const PROJECTID = 'refpds-poc-1';
const COLLECTION_NAME = 'Orders';
const firestore = new Firestore({
  projectId: PROJECTID,
  timestampsInSnapshots: true,
});



exports.handleCDCEvent = (payload, context) => {
  const strMsg = payload.data
    ? Buffer.from(payload.data, 'base64').toString()
    : '{}';
  let msg = JSON.parse(strMsg);

  console.log("Message received: " + strMsg);

  let transactionType = msg.transactionType;

  console.log("transactionType: " + transactionType);

  let docID = msg.order_number.toString();
  
  delete msg['transactionType'];
  if(transactionType === 'INSERT' || transactionType === 'UPDATE'){
    firestore.collection(COLLECTION_NAME)
      .doc(docID)
      .set(msg)
      .then(doc => {
        console.log('Written to firestore: ' + docID)
      }).catch(err => {
        console.error(err);
      });
  } else if(transactionType === 'DELETE'){
    firestore.collection(COLLECTION_NAME)
    .doc(docID)
    .delete()
    .then(doc => {
      console.log('Deleted from firestore: ' + docID)
    }).catch(err => {
      console.error(err);
    });
  }

}