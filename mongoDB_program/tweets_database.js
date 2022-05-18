
// make a connection to the database server
conn = new Mongo();

// set the default database
db = conn.getDB("assignment");

print("Q1=======")

print("general:"+db.tweets.find( {$and: [{replyto_id: {$exists: false}}, {retweet_id: {$exists: false}}]}).count())

print("reply:"+db.tweets.find({replyto_id: {$exists: true}}).count())

print("retweet:"+db.tweets.find({retweet_id: {$exists: true}}).count())





print("Q2=======")
// optionally timing the execution
var start = new Date()
cursor = db.tweets.aggregate([

{$match:{hash_tags: {$exists: true}}},
{$match:{$or: [ {$and: [{replyto_id: {$exists: false}}, {retweet_id: {$exists: false}}  ]}, {replyto_id: {$exists: true}}  ]}},
{$unwind: "$hash_tags"},
{$group : {_id : "$hash_tags.text",count : { $sum : 1}}},
{$sort : {count : -1}},
{$limit : 5},

])
while ( cursor.hasNext() ) {
    printjson( cursor.next() );
}
var end = new Date()
print("\nQuery Execution time: " + (end - start) + "ms")




print("Q3=======")
db.tweets.aggregate([

{$project:{
    _id : 0,
    id: 1,
    replyto_id: 1,
    created_at: {
                $dateFromString: {
                dateString: '$created_at', }
           }
       }
   },
   {$out:'reply_v2'},
   
]);

db.reply_v2.aggregate([

{$group : {_id : "$replyto_id" , first : {$min : "$created_at"} }},

{$project : {_id : 1, first : 1}},

{$out : 'reply_v3'},

]);


var start = new Date()
cursor = db.reply_v3.aggregate([

{$lookup : {
    from: "reply_v2",
    localField : "_id",
    foreignField : "id",
    as: "duration"
    }},


{$unwind : "$duration"},
{$project : {id : "$_id", duration : {$divide : [{ $subtract : ["$first", "$duration.created_at"]}, 1000]} , "_id" : 0 }},
{$sort : {duration: -1}},
{$limit : 1}

])
while ( cursor.hasNext() ) {
    printjson( cursor.next() );
}
var end = new Date()
print("\nQuery Execution time: " + (end - start) + "ms")
db.reply_v3.drop()
db.reply_v2.drop();






print("Q4=======")
db.tweets.aggregate([

{$project:{

    retweet_id : 1,
    }
    },
     {$match:{retweet_id:{$exists:true}}},
    {
        $out:'retweet',
    }
])


db.tweets.aggregate([
{$match:{retweet_id:{$exists: false}}},
{$project:{
    id : 1,
    retweet_count : 1
    }},
    {$out : 'retweet1'}
])

var start = new Date()
cursor = db.retweet1.aggregate([
{$lookup : {
    from: "retweet",
    localField : "id",
    foreignField : "retweet_id",
    as: "retweetex"
    }},
{$project : {retweet_id: 1, retweet_count : 1, retweet_db : {$size : "$retweetex"}}},

{$match : { $expr : { $gt :["$retweet_count", "$retweet_db"]}}},
{$count:"retweet_id"}
])
while ( cursor.hasNext() ) {
    printjson( cursor.next() );
}
var end = new Date()
print("\nQuery Execution time: " + (end - start) + "ms")
db.retweet.drop()
db.retweet1.drop()






print("Q5=======")
db.tweets.aggregate([

{$match : {$or : [{retweet_id : {$exists : true}}, {replyto_id : {$exists : true}} ]}},
{$project:{
    retweet_id:1,
    replyto_id:1,
    _id: 0
    }},
{$out:"check_list"}
])


var start = new Date()
cursor = db.check_list.aggregate([
{$lookup : {from : "tweets", localField : "retweet_id", foreignField : "id", as : "result1"}},
{$lookup : {from : "tweets", localField : "replyto_id", foreignField : "id", as : "result2"}},
{$match : {$and:[{result1 : [],result2 :[]}]}},

{$count : "result"}
])
while ( cursor.hasNext() ) {
    printjson( cursor.next() );
}
var end = new Date()
print("\nQuery Execution time: " + (end - start) + "ms")
db.check_list.drop()






print("Q6=======")
db.tweets.aggregate([

{$match : {$or : [{retweet_id : {$exists : true}}, {replyto_id : {$exists : true}} ]}},

{$group: {"_id":0, "list1":{ $addToSet: "$retweet_id"}, "list2":{$addToSet:"$replyto_id"}}},

{$project: { double_id : {$setUnion : ["$list1","$list2"]}}},

{$unwind : "$double_id"},

{$project : {double_id : 1, _id : 0}},

{$out : "double_list"}

]);

db.tweets.aggregate([

{ $match : {$and : [ {replyto_id : {$exists : false} }, {retweet_id: {$exists : false} }]}},

{$project : {id: 1, _id:0}},

{$out : "no_list"}

]);
var start = new Date()
cursor = db.no_list.aggregate([

{$lookup : {from : "double_list", localField : "id" , foreignField : "double_id", as : "result_list"}},

{$match : {result_list : []}},

{$count : "result_list"}

]);



while ( cursor.hasNext() ) {
    printjson( cursor.next() );
}
var end = new Date()
print("\nQuery Execution time: " + (end - start) + "ms")

db.double_list.drop()
db.no_list.drop()
