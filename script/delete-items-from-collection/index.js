const { Firestore } = require('@google-cloud/firestore');

/**
 * INIT DB
 */
const DB = new Firestore({
  projectId: 'store-csv-file',
  keyFilename: './credentials.json',
});

let BATCH_SIZE = 50000;
let counter = 1;
let batchCommitsCounter = 1;
let deletedItemsCounter = 0;

const deleteItems = (itemsLimit) => {
  return DB.collection("blacklist")
    .limit(itemsLimit)
    .get()
    .then(res => {
      if (res.empty) {
        console.log(`ALL is deleted TOTAL: ${deletedItemsCounter}`);
        return;
      }

      const batchCommits = [];
      let batch = DB.batch();
      res.docs.forEach((element) => {
        counter += 1;
        if (counter % BATCH_SIZE === 0) {
          console.log(`DELETED ${counter}`)
        }

        batch.delete(element.ref);
        if (counter % 500 === 0) {
          deletedItemsCounter += batch._ops.length;
          batchCommits.push(batch.commit());
          batch = DB.batch();
        }
      });

      console.log(`DELETING batch ${batchCommitsCounter++}`);
      if (deletedItemsCounter % BATCH_SIZE === 0) {
        console.log(`Deleted items: ${deletedItemsCounter}`);
      }

      const maxItemsForCommits = itemsLimit / 500;
      if (batch._ops.length < 500 && batchCommits.length < maxItemsForCommits) {
        deletedItemsCounter += batch._ops.length;
        batchCommits.push(batch.commit());
      }

      return Promise.all(batchCommits).then(() => {
        deleteItems(itemsLimit);
      });
      // batch.commit().then(() => { deleteItems(itemsCount) });
    });
}
async function showCollectionSize(collection) {
  console.log('showCollection 1')
  // const doc = await DB.collection(collection).doc('zusum@baskolise.ne').get();
  // console.log('?????', doc.data())
  const collectionSnapshot = await DB.collection(collection).get();
  console.log(`=>> Collection Size: ${collectionSnapshot.size}`)
  console.log('showCollection 2')
}
// showCollectionSize('users')
deleteItems(BATCH_SIZE);