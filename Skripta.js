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
                $match: {
                    "publishers": "Valve"
                }
            }
            ,
            {
                $group: {
                    "_id": null, 
                    "averageRating": {$avg: "$rating"}
                }
            }
        ]
    );
                 
    retVal = retVal.map( function(retVal) { return retVal.averageRating; })[0];

    return retVal;
};



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



////////////////////////////////////////////////////////////////////////////////////////////////////
// Poziv ispisa 
////////////////////////////////////////////////////////////////////////////////////////////////////


pitanje1();



































