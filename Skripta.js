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


////////////////////////////////////////////////////////////////////////////////////////////////////
// Ispisne funkcije
////////////////////////////////////////////////////////////////////////////////////////////////////


function pitanje1 () {
    var prosekSvih = getBestPublishers();
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



////////////////////////////////////////////////////////////////////////////////////////////////////
// Poziv ispisa 
////////////////////////////////////////////////////////////////////////////////////////////////////


pitanje1();

pitanje2();






////////////////////////////////////////////////////////////////////////////////////////////////////
// Development 
////////////////////////////////////////////////////////////////////////////////////////////////////
















