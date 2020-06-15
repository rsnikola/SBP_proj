////////////////////////////////////////////////////////////////////////////////////////////////////
// Upitne funkcije
////////////////////////////////////////////////////////////////////////////////////////////////////


function getPublisherRatingAverage () {
    var retVal = db.steam_aggregate.aggregate(
        [
            {
                $project: {
                    "publishers": "$publishers",  
                    "rating": {$subtract: ["$spy_data.positive", "$spy_data.negative"]}
                }
            }
            ,
            {
                $group: {
                    "_id": "$publishers", 
                    "rating": {$avg: "$rating"}
                }
            }
            ,
            {
                $group: {
                    "_id": null, 
                   "averageOverall": {$avg: "$rating"}
                }
            }
        ]
    );
                 
    retVal = retVal.map( function(retVal) { return retVal.averageOverall; })[0];

    return retVal;
};


function getValveRatingAverage () {
    var retVal = db.steam_aggregate.aggregate(
        [
            {
                $project: {
                    "publishers": "$publishers",  
                    "rating": {$subtract: ["$spy_data.positive", "$spy_data.negative"]}
                }
            }
            ,
            {
                $group: {
                    "_id": "$publishers", 
                    "rating": {$avg: "$rating"}
                }
            }
            ,
            {
                $match: {
                    "_id": "Valve"
                }
            }
        ]
    );
                 
    retVal = retVal.map( function(retVal) { return retVal.rating; })[0];

    return retVal;
};


function getDevWithMostHoursAverage () {
    var retVal = db.steam_aggregate.aggregate(
        [
            {
                $project: {
                    "developers": "$developers", 
                    "average_forever": {$max: "$spy_data.average_forever"}, 
                    "name": "$name"
                }
            }
            ,
            {
                $group: {
                    "_id": "$developers", 
                    "average_forever": {$push: "$average_forever"}
                }
            }
            ,
            {
                $project: {
                    "_id": "$_id", 
                    "average_forever": {$max: "$average_forever"}
                }
            }
            ,
            {
                $sort: {"average_forever": -1}
            }
            ,
            {
                $limit: 1
            }
            ,
            {
                $project: {
                    "_id": "$_id"
                }
            }
        ]
    );
    
    return retVal.map( function(retVal) { return retVal._id; })[0];
}


function getDevWithMostHoursMean () {
    var retVal = db.steam_aggregate.aggregate(
        [
            {
                $project: {
                    "developers": "$developers", 
                    "mean_forever": {$max: "$spy_data.median_forever"}, 
                    "name": "$name"
                }
            }
            ,
            {
                $group: {
                    "_id": "$developers", 
                    "mean_forever": {$push: "$mean_forever"}
                }
            }
            ,
            {
                $project: {
                    "_id": "$_id", 
                    "mean_forever": {$max: "$mean_forever"}
                }
            }
            ,
            {
                $sort: {"mean_forever": -1}
            }
            ,
            {
                $limit: 1
            }
            ,
            {
                $project: {
                    "_id": "$_id"
                }
            }
        ]
    );
    
    return retVal.map( function(retVal) { return retVal._id; })[0];
}


function getPublisherWithMostDlc () {
    var retVal = db.steam_aggregate.aggregate(
        [
            {
                $project: {
                    "publishers": "$publishers", 
                    "dlc": {$size: {"$ifNull": ["$dlc", []]}}, 
                    
                }
            }
            ,
            {
                $group: {"_id": "$publishers", "dlc": {$sum: "$dlc"}}
            }
            ,
            {
                $sort: {"dlc": -1}
            }
            ,
            {
                $limit: 1
            }
        ]
    );
            
    return retVal.map( function(retVal) { return retVal._id; })[0];
}


function getMostBangForBuckMed () {
    var retVal = db.steam_aggregate.aggregate(
        [
            {
                $project: {
                    "name": "$name", 
                    "price": "$spy_data.price", 
                    "average_hours": "$spy_data.median_forever",
                    "is_free": "$is_free"
                }
            }
            ,
            {
                $match: {
                    "is_free": "False", 
                    "price": {$ne: 0}, 
                    "average_hours": {$ne: 0}
                }
            }
            ,
            {
                $project: {
                    "name": "$name", 
                    "price": "$price", 
                    "average_hours": "$average_hours",
                    "price_per_hour": {"$ifNull": [{$divide: [{"$ifNull": ["$average_hours", 1.0]}, {"$ifNull": ["$price", 1.0]}]}, 0.0]}
                }
            }
            ,
            {
                $sort: {
                    "price_per_hour": -1
                }
            }
            ,
            {
                $limit: 10
            }
            ,
            {
                $project: {
                    "_id": "$name"
                }
            }
        ]
    );
            
    retVal = retVal.map( function(retVal) { return retVal._id; });
    
    var temp = ["", "", "", "", "", "", "", "", "", ""];
           
    for (var i = 0; i < 10; ++i) {
        temp[i] = retVal[i];
    }
    retVal = temp;
    
    return retVal;
}


function getMostBangForBuckAvg () {
    retVal = db.steam_aggregate.aggregate(
        [
            {
                $project: {
                    "name": "$name", 
                    "price": "$spy_data.price", 
                    "average_hours": "$spy_data.average_forever",
                    "is_free": "$is_free"
                }
            }
            ,
            {
                $match: {
                    "is_free": "False", 
                    "price": {$ne: 0}, 
                    "average_hours": {$ne: 0}
                }
            }
            ,
            {
                $project: {
                    "name": "$name", 
                    "price": "$price", 
                    "average_hours": "$average_hours",
                    "price_per_hour": {"$ifNull": [{$divide: [{"$ifNull": ["$average_hours", 1.0]}, {"$ifNull": ["$price", 1.0]}]}, 0.0]}
                }
            }
            ,
            {
                $sort: {
                    "price_per_hour": -1
                }
            }
            ,
            {
                $limit: 10
            }
            ,
            {
                $project: {
                    "_id": "$name"
                }
            }
        ]
    );
            
    retVal = retVal.map( function(retVal) { return retVal._id; });
    
    var temp = ["", "", "", "", "", "", "", "", "", ""];
           
    for (var i = 0; i < 10; ++i) {
        temp[i] = retVal[i];
    }
    retVal = temp;
    
    return retVal;
}


////////////////////////////////////////////////////////////////////////////////////////////////////
// Ispisne funkcije
////////////////////////////////////////////////////////////////////////////////////////////////////


function pitanje1 () {
    var prosekSvih = getPublisherRatingAverage();
    var prosekValve = getValveRatingAverage();
    
    print ("Pitanje 1");
    print ("");
    print ("Da li su igre izdavacke kuce Valve u proseku bolje ocenjene od prosecne ocene ostalih izdavackih kuca?");
    print ("Prosek svih proseka ocena pojedinacnih izdavackih kuca: " + prosekSvih);
    print ("Prosek svih ocena Valve igara: " + prosekValve);
    print ("");
    print ("Zakljucak: ");
    print ("Valve igre u proseku " + ((prosekSvih < prosekValve) ? ("jesu") : ("nisu")) + " bolje ocenjene od igara ostalih izdavackih kuca. ");
    print ("");
}


function pitanje2 () {
    var devSaNajviseProsecno = getDevWithMostHoursAverage ();
    var devSaNajviseSrednje = getDevWithMostHoursMean ();
    
    print ("Pitanje 2");
    print ("");
    print ("Koji razvojni studio ima najviše sati u svojim igrama?");
    print ("Razvoji studio sa najvecim prosekom sati u igrama je: " + devSaNajviseProsecno);
    print ("Razvoji studio sa najvecom srednjom vrednosti sati u igrama je: " + devSaNajviseSrednje);
    print ("");
    print ("Zakljucak: ");
    print ("Zavisno od kriterijuma, odgovor je ili " + devSaNajviseProsecno + ", ili " + devSaNajviseSrednje + ".");
    print ("");
}


function pitanje3 () {
    var pabliserSaNajviseDlc = getPublisherWithMostDlc ();
    
    print ("Pitanje 3");
    print ("");
    print ("Koji izdavaci imaju najveci broj dlc?");
    print ("Upit je pokazao da je u pitanju: " + pabliserSaNajviseDlc + ". ");
    print ("");
    print ("Zakljucak: ");
    print ("Izdavac sa najviše DLC je: " + pabliserSaNajviseDlc + ".");
    print ("");
}


function pitanje4 () {
    var prosek = getMostBangForBuckAvg ();
    var srednja = getMostBangForBuckMed();
    
    print ("Pitanje 4");
    print ("");
    print ("Koje igre su najisplativije? (broj sati u igri/cena)");
    print ("Upiti su vršeni po dva kiiterijuma. ");
    print ("Prvi kriterijum je bio prosek sati u igri po ceni: " + prosek + ". ");
    print ("Drugi kriterijum je bio srednja vrednost broja sati u igri po ceni: " + srednja + ". ");
    print ("");
    print ("Zakljucak: ");
    print ("U zavisnosti od kriterijuma, najisplativijom igrom se moze smatrati " + prosek[0] + ", tj. " + srednja[0] + ". ");
    print ("");
}



////////////////////////////////////////////////////////////////////////////////////////////////////
// Poziv ispisa 
////////////////////////////////////////////////////////////////////////////////////////////////////


pitanje1();

pitanje2();

pitanje3();

pitanje4();




////////////////////////////////////////////////////////////////////////////////////////////////////
// Development 
////////////////////////////////////////////////////////////////////////////////////////////////////



db.steam_aggregate.aggregate(
        [
            {
                $project: {
                    "name": "$name", 
                    "is_free": "$is_free", 
                    "diff": {$differen}
                }
            }
        ]
    );



















