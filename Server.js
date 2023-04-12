import { MongoClient } from 'mongodb';
import { Writable } from 'stream';



async function main() {
    const uri = "mongodb+srv://mongosample:OAKVKSwTLdnUjyO1@sample_analytics.xa9lb2l.mongodb.net"
    const client = new MongoClient(uri);
    try {
        await client.connect();

        const pipeline = [
              {
                  '$match': {
                      'operationType': 'insert',
                      'fullDocument.account_id': 1111,
                  }
              }
        ];

        //get accounts data putting restriction 
        //await getAccountData(client, 600000000,pipeline);
       // await getAccountsDataUsingStreamAPI(client, 600000000,pipeline);
       await getAccountsDataUsingHasNext(client, 600000000,pipeline);

    } finally {
        await client.close();
    }
}

main().catch(console.error);

function closeChangeStream(timeInMs = 600000, changeStream) {
    return new Promise((resolve) => {
        setTimeout(() => {
            console.log("Closing the change stream");
            resolve(changeStream.close());
        }, timeInMs)
    })
};

async function getAccountData(client, timeInMs = 600000000,pipeline = []) {
    const collection = client.db("sample_analytics").collection("accounts");
    const changeStream = collection.watch(pipeline);
    changeStream.on('change', (next) => {
        console.log(next);
    });
    await closeChangeStream(timeInMs, changeStream);
}

async function getAccountsDataUsingHasNext(client, timeInMs = 60000000, pipeline = []) {
    const collection = client.db("sample_analytics").collection("accounts");
    const changeStream = collection.watch(pipeline);
    closeChangeStream(timeInMs, changeStream);
    try {
        while (await changeStream.hasNext()) {
            console.log(await changeStream.next());
        }
    } catch (error) {
        if (changeStream.isClosed()) {
            console.log("the change stream is closed . will not wait on any more changes")
        } else {
            throw error;
        }
    }
}

async function getAccountsDataUsingStreamAPI(client, timeInMs = 60000000, pipeline = []) {
    const collection = client.db("sample_analytics").collection("accounts");
    const changeStream = collection.watch(pipeline);
    changeStream.stream().pipe(
        new Writable({
            objectMode: true,
            write: function(doc,_,cb){
                 console.log(doc);
                 cb();
            }
        })
    );
    await closeChangeStream(timeInMs, changeStream);
}



