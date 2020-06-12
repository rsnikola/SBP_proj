db.getCollection('steam_app_data').aggregate([
    {
            $lookup: {
                from:'steamspy_data', 
                localField: 'name', 
                foreignField: 'name',
                as: 'spy_data'
            }
    }
    ,
    {
        $project: {
            "owners": {$split: [{$arrayElemAt: ["$spy_data.owners", 0]}, " .. "]}, 
            "name": "$name", 
            "required_age": "$required_age", 
            "is_free": "$is_free", 
            "controller_support": "$controller_support", 
            "dlc": "$dlc", 
            "developers": "$developers", 
            "publishers": "$publishers", 
            "categories": "$categories", 
            "genres": "$genres", 
            "recommendations": "$recommendations", 
            "positive": "$spy_data.positive", 
            "negative": "$spy_data.negative",
            "price": "$spy_data.price", 
            "average_forever": "$spy_data.average_forever", 
            "median_forever": "$spy_data.median_forever"
        }
    }
    ,
    {
        $project: {
            "owners_min": {$arrayElemAt: ["$owners", 0]}, 
            "owners_max": {$arrayElemAt: ["$owners", 1]},
            "name": "$name", 
            "required_age": "$required_age", 
            "is_free": "$is_free", 
            "controller_support": "$controller_support", 
            "dlc": "$dlc", 
            "developers": "$developers", 
            "publishers": "$publishers", 
            "categories": "$categories", 
            "genres": "$genres", 
            "recommendations": "$recommendations", 
            "positive": {$arrayElemAt: ["$positive", 0]}, 
            "negative": {$arrayElemAt: ["$negative", 0]},
            "price": {$arrayElemAt: ["$price", 0]}, 
            "average_forever": {$arrayElemAt: ["$average_forever", 0]}, 
            "median_forever": {$arrayElemAt: ["$median_forever", 0]}
        }
    }
    ,
    {
        $project: {
            "owners_min": {$split: ["$owners_min", ","]}, 
            "owners_max": {$split: ["$owners_max", ","]},
            "name": "$name", 
            "required_age": "$required_age", 
            "is_free": "$is_free", 
            "controller_support": "$controller_support", 
            "dlc": "$dlc", 
            "developers": "$developers", 
            "publishers": "$publishers", 
            "categories": "$categories", 
            "genres": "$genres", 
            "recommendations": "$recommendations", 
            "positive": "$positive", 
            "negative": "$negative",
            "price": "$price", 
            "average_forever": "$average_forever", 
            "median_forever": "$median_forever"
        }
    }
    ,
    {   
        $project: {
            "owners_min": {
                $reduce: {
                    'input': '$owners_min', 
                    'initialValue': '', 
                    'in': {
                            '$concat': [
                             '$$value',
                            {'$cond': [{'$eq': ['$$value', '']}, '', '']}, 
                            '$$this'
                        ]
                    }
                }
            }
            ,
            "owners_max": {
                $reduce: {
                    'input': '$owners_max', 
                    'initialValue': '', 
                    'in': {
                            '$concat': [
                             '$$value',
                            {'$cond': [{'$eq': ['$$value', '']}, '', '']}, 
                            '$$this'
                        ]
                    }
                }
            }, 
            "name": "$name", 
            "required_age": "$required_age", 
            "is_free": "$is_free", 
            "controller_support": "$controller_support", 
            "dlc": "$dlc", 
            "developers": "$developers", 
            "publishers": "$publishers", 
            "categories": "$categories", 
            "genres": "$genres", 
            "recommendations": "$recommendations", 
            "positive": "$positive", 
            "negative": "$negative",
            "price": "$price", 
            "average_forever": "$average_forever", 
            "median_forever": "$median_forever"
        }
    }
    ,
    {
        $project: {
            "owners_min": {$toInt: "$owners_min"}, 
            "owners_max": {$toInt: "$owners_max"}, 
            "name": "$name", 
            "required_age": "$required_age", 
            "is_free": "$is_free", 
            "controller_support": "$controller_support", 
            "dlc": "$dlc", 
            "developers": "$developers", 
            "publishers": "$publishers", 
            "categories": "$categories", 
            "genres": "$genres", 
            "recommendations": "$recommendations", 
            "spy_data.positive": "$positive", 
            "spy_data.negative": "$negative",
            "spy_data.price": "$price", 
            "spy_data.average_forever": "$average_forever", 
            "spy_data.median_forever": "$median_forever"
        }
    }
]);



















function pitanje1 () {
    var prosecna_ocena_po_developeru = db.getCollection('steamspy_data').aggregate(
        [
            {
                $project: {"_id":"$name", "developers":"$developer", "diff":{$subtract:["$positive", "$negative"]}}
            }
            ,
            {
                $group: {"_id":"$developers", "average":{$avg:"$diff"}}
            }
            ,
            {
                $group: {"_id":null, "rez":{$avg:"$average"}}
            }
            , 
            {
                $project: {"_id":"$rez"}
            }
        ]
            
    );
    var prosecna_ocena_valve = db.getCollection('steamspy_data').aggregate(
        [
            {
                $project: {"_id":"$name", "developers":"Valve", "diff":{$subtract:["$positive", "$negative"]}}
            }
            ,
            {
                $group: {"_id":"$developers", "rez":{$avg:"$diff"}}
            }
            , 
            {
                $project: {"_id":"$rez"}
            }
        ]
    )
            
    prosecna_ocena_po_developeru = prosecna_ocena_po_developeru.map( function(u) { return u._id; })[0];
    prosecna_ocena_valve = prosecna_ocena_valve.map( function(u) { return u._id; })[0];
    //prosecna_ocena_po_developeru;
    // prosecna_ocena_valve;

    var valve_u_odnosu_na_ostale = prosecna_ocena_valve / prosecna_ocena_po_developeru
    print ("Igre developera Valve su " + valve_u_odnosu_na_ostale + " puta bolje ocenjene u odnosu na ostale developere. ")
    
}


function pitanje2 () {
    var a = db.getCollection('steamspy_data').aggregate(
        [
            {
             $project: {"_id" : "$name", "average_forever":"$average_forever", "median_forever":"$median_forever", "developer":"$developer"}
            }
            ,
            {
                 $group: {"_id": "$developer", "average_forever": {$avg:"$average_forever"}}
               }
               ,
               {
                 $sort: {"average_forever": -1}
               }
               ,
               {
                 $limit: 10
               }
        ]
    )
     var i;
     for (i = 0; i < 10; ++i) {
        print (i + 1 + ": "); 
        print ("Developer: " + a._batch[i]._id);
        print ("Prosecan broj sati u igrama: " + a._batch[i].average_forever);
        print ("");
     }
}


db.getCollection('steam_app_data').aggregate(
    [
        {
         $project: {"_id": "$_id", "dlc":"$dlc", "hasDlc": {$ne : ["", "$dlc"] }, "publisher": "$publishers"}
        }
        ,
        {
         $match: {"hasDlc": true}
        }
        ,
        {
         $project: {"_id": "$_id", "dlc":"$dlc", "publisher": "$publisher"}
        }
    ]
)

////////////////////////////////////////////////////////////////////////////////
// 1. Da li su igre developera Valve bolje ocenjene od igara prosecnog developera?
// pitanje1();
////////////////////////////////////////////////////////////////////////////////

////////////////////////////////////////////////////////////////////////////////
// 2. Koji razvojni studio ima najvise sati u svojim igrama? 
// pitanje2();
////////////////////////////////////////////////////////////////////////////////


db.getCollection('steamspy_data').find({});
db.getCollection('steam_app_data').find({});
db.getCollection('steam_app_data').aggregate(
    [
        {
            $lookup: {
                from:'steamspy_data', 
                localField: 'name', 
                foreignField: 'name',
                as: 'spy_data'
            }
        }
        ,
        {
            $project: {
                "_id": "$_id",
                "name": "$name", 
                "required_age": "$required_age", 
                "is_free": "$is_free", 
                "controller_support": "$controller_support", 
                "dlc": "$dlc", 
                "developers": "$developers", 
                "publishers": "$publishers", 
                "categories": "$categories", 
                "genres": "$genres", 
                "recommendations": "$recommendations", 
                "spy_data.positive": "$spy_data.positive", 
                "spy_data.negative": "$spy_data.negative",
                "spy_data.owners": "$spy_data.owners", 
                "spy_data.price": "$spy_data.price", 
                "spy_data.average_forever": "$spy_data.average_forever", 
                "spy_data.median_forever": "$spy_data.median_forever"
            }
        }
//         }
//         ,
//         {
//             $out: "steam_aggregate"
//         }
    ]
);







db.getCollection('temp').find({})























































































