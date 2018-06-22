/**************************************************************
Small nodeJs script to answer the problem given in the exercise
it works as the following:
1. create a mongodb database where to persist necessary info
2. simulate requests to bitcoind listsinceblock RPC
    2.1- read bitcoind query replies from given files
    2.2- persist each query replies in it as received
    2.3- analyze reply and store TXs accordingly
    2.4- after having treated all responses from bitcoind
         calculate the balance of each account address in wallet
3. finalize the script, drop table and persisted info from DB
****************************************************************/


(async () => {

  /*****************************************************************/
  /*        1 create database for persisting data                  */
  /*****************************************************************/
  var MongoClient = require('mongodb').MongoClient;
  var url = "mongodb://mongo/krakenTestDB";
  const db = await MongoClient.connect(url);
  console.log("Database 'krakenTestDB' created!");
  var dbo = db.db("krakenTestDB");
  try
  {
     /*  create table for persisting replies to our      */
     /*  'listsinceblock' requests to the bitcoin deamon */
     await dbo.createCollection("lsbResponses");
     console.log("Collection 'lsbResponses' created!");
     /*  create table for persisting transactions     */
     await dbo.createCollection("deposits");
     console.log("Collection 'transactions' created!");
     /*  create table for keeping balance info   */
     await dbo.createCollection("balances");
     console.log("Collection 'balances' created!");



     /*****************************************************************/
     /*          2. start silulating the requests                     */
     /*            to bitcoind listsinceblock RPC                     */
     /*****************************************************************/
     /* 2.1 read data from given files */
     const fs = require('fs');
     var clientArray = [];
     var lsbArray = [];
     let rawdata1 = fs.readFileSync('data/transactions-1.json');
     let rawdata2 = fs.readFileSync('data/transactions-2.json');
     lsbArray.push(JSON.parse(rawdata1));
     lsbArray.push(JSON.parse(rawdata2));
     let rawdata3 = fs.readFileSync('address.json');
     clientArray = JSON.parse(rawdata3);

     /* loop over the periodical requests and given replies  */
     for(var i=0;i<lsbArray.length;i++){ //

       /*****************************************************************/
       /* 2.2 persist in DB the replie from given file */
       await dbo.collection("lsbResponses").insertOne(lsbArray[i]);
       console.log((i+1)+"th response from bitcoind is persisted in DB");

       /*****************************************************************/
       /* 2.3 analyze and persist the transactions */
       /*  get previously received and not yet confirmed TXs
           and check if we received them again in the current response
           if yes, update their confirmation number.
           if not, it means they exceeded the targetConfirmation.
           updating their confirmation number to 6 is enough for such cases   */

       //update old unconfirmed deposits according to new ones
       var allPrevUnconfirmedDepos = await dbo.collection("deposits").find({ confirmations : { $gte :  0  }, confirmations: {$lt : 6}}).toArray();
       var resultAllPrevDepos = await dbo.collection("deposits").find().toArray();
       console.log(resultAllPrevDepos.length + " deposits exists in DB");
       console.log(allPrevUnconfirmedDepos.length + " not yet unconfirmed deposit exists in DB");
       //update in DB
       var updatedPrevDepos = checkNUpdatePrevUnconfirmeds(allPrevUnconfirmedDepos, lsbArray[i].transactions);
       for(var n=0;n<updatedPrevDepos.length;n++)
       {
         var myquery = { txid: updatedPrevDepos[n].txid };
         var newvalues = { $set: {confirmations: updatedPrevDepos[n].confirmations} };
         await dbo.collection("deposits").updateOne(myquery, newvalues);
       }
       console.log( updatedPrevDepos.length + " previously received unconfirmed transactions were updated");



       //check if new transactions are valid
       var insertCount=0;
       var updateCount=0;
       var sameCount=0;
       for(var k=0; k<lsbArray[i].transactions.length; k++){
         //check if txID is already in DB
         var resultCheckTX = await dbo.collection("deposits").findOne({txid: lsbArray[i].transactions[k].txid});
         //if its already in DB, it may be an already CONFIRMED deposit
         if(resultCheckTX){
           //check if its not a chain reorg, and that its confirmation did not decrease
           if(lsbArray[i].transactions[k].blockhash == resultCheckTX.blockhash){
             sameCount++;
           }else{
             //previously confirmed transaction's block was modified due to chain reorg
             await dbo.collection("deposits").deleteOne({txid: lsbArray[i].transactions[k].txid});
             await dbo.collection("deposits").insertOne(lsbArray[i].transactions[k]);
             updateCount++;
           }
         }
         //insert if tx is not in conflict
         else if(lsbArray[i].transactions[k].confirmations >= 0){
           await dbo.collection("deposits").insertOne(lsbArray[i].transactions[k]);
           insertCount++;
         }
       }
       console.log("all " +lsbArray[i].transactions.length+ " transactions from the " + (i+1) + "th 'listsinceblock' reply are now analyzed");
       console.log(insertCount + " of them were inserted");
       console.log(updateCount + " of them were updates");
       console.log(sameCount + " of them were already received. did not impact wallet balance");
     }

     /*****************************************************************/
     /*  2.4 calculate balance according to      */
     /*  the valid transactions persisted in DB  */
     //print known clients' balances
     for(var m=0;m<clientArray.length;m++){
       var query = { address: clientArray[m].address };
       var result = await dbo.collection("deposits").find(query).toArray();
       console.log("Deposited for "+clientArray[m].name+": count="+result.length + " sum=" + sumAmount(result).toFixed(8));
     }
     //print unknown address balances
     var resultUR = await dbo.collection("deposits").find({ address : { $nin: clientArray.map(a => a.address) }}).toArray();
     console.log("Deposited without reference: count="+resultUR.length + " sum=" + sumAmount(resultUR).toFixed(8));
     //print general max and min deposit
     var resultAllDepos = await dbo.collection("deposits").find().toArray();
     var maxMin = getMaxMin(resultAllDepos);
     console.log("Smallest valid deposit:"+maxMin.min);
     console.log("Largest valid deposit:"+maxMin.max);



     /*****************************************************************/
     /*         3. once the script finalized,                         */
     /*         drop the dable and delete data inserted               */
     /*****************************************************************/
     await dbo.collection("lsbResponses").drop();
     console.log("Collection 'lsbResponses' deleted");
     await dbo.collection("deposits").drop();
     console.log("Collection 'deposits' deleted");
  }
  finally {
     db.close();
  }
})().catch(err => {
    console.error(err);
});






/*****************************************************************/
/*         Appendix: utils functions                             */
/*****************************************************************/
function sumAmount(txArray) {
  var sumAmount = 0;
  for(var i=0;i<txArray.length;i++){
    sumAmount += txArray[i].amount;
  }
  return sumAmount;
}

function getMaxMin(txArray){
  var amountArray = txArray.map(a => a.amount);
  return {min:Math.min( ...amountArray ), max:Math.max( ...amountArray )};
}

function checkNUpdatePrevUnconfirmeds(previouslyUnconfirmedTXs, newTXs){
  for(var i=0;i<previouslyUnconfirmedTXs.length;i++){
    flag = 0;
    for(var k=0;k<newTXs.length;k++){
      if(previouslyUnconfirmedTXs[i].txid==newTXs[k].txid){
        previouslyUnconfirmedTXs[i]=newTXs[k];
        flag=1;
      }
    }
    if(flag==0){
      previouslyUnconfirmedTXs[i].confirmations = 6;
    }
  }
  return previouslyUnconfirmedTXs;
}
