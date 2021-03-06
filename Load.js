db.getCollection('steam_app_data').aggregate([
    {
            $lookup: {
                from:'steamspy_data', 
                localField: 'name', 
                foreignField: 'name',
                as: 'spy_data'
            }
    }
//     ,
//     {
//         $match: {"name": "Counter-Strike"}
//     }
    ,
    {
        $project: {
            "owners": {$split: [{$arrayElemAt: ["$spy_data.owners", 0]}, " .. "]}, 
            "name": "$name", 
            "required_age": "$required_age", 
            "is_free": "$is_free", 
            "controller_support": "$controller_support", 
            "dlc": {$split: ["$dlc", "["]}, 
            "developers": {$split: ["$developers", "["]}, 
            "publishers": {$split: ["$publishers", "["]}, 
            "categories": {$split: ["$categories", "[{"]}, 
            "genres": {$split: ["$genres", "[{"]}, 
            "recommendations": {$split: ["$recommendations", "{'total': "]}, 
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
            "dlc": {$split: [{$arrayElemAt: ["$dlc", 1]}, "]"]}, 
            "developers": {$split: [{$arrayElemAt: ["$developers", 1]}, "]"]}, 
            "publishers": {$split: [{$arrayElemAt: ["$publishers", 1]}, "]"]}, 
            "categories": {$split: [{$arrayElemAt: ["$categories", 1]}, "}]"]}, 
            "genres": {$split: [{$arrayElemAt: ["$genres", 1]}, "}]"]}, 
            "recommendations": {$split: [{$arrayElemAt: ["$recommendations", 1]}, "}"]}, 
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
            "dlc": {$arrayElemAt: ["$dlc", 0]}, 
            "developers": {$arrayElemAt: ["$developers", 0]}, 
            "publishers": {$arrayElemAt: ["$publishers", 0]}, 
            "categories": {$arrayElemAt: ["$categories", 0]}, 
            "genres": {$arrayElemAt: ["$genres", 0]}, 
            "recommendations": {$arrayElemAt: ["$recommendations", 0]}, 
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
            "dlc": {$split: ["$dlc", ", "]}, 
            "developers": {$split: ["$developers", ", "]}, 
            "publishers": {$split: ["$publishers", ", "]}, 
            "categories": {$split: ["$categories", "}, {"]}, 
            "genres": {$split: ["$genres", "}, {"]}, 
            "recommendations": {$toInt: "$recommendations"}, 
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
            "positive": "$positive", 
            "negative": "$negative",
            "price": "$price", 
            "average_forever": "$average_forever", 
            "median_forever": "$median_forever"
        }
    }
    ,
    {
        $unwind: "$categories"
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
            "categories": {$split: ["$categories", "'description': '"]}, 
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
            "categories": {$arrayElemAt: ["$categories", 1]}, 
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
            "categories": {$split: ["$categories", "'"]}, 
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
            "categories": {$arrayElemAt: ["$categories", 0]}, 
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
        $group: {
            "_id": "$_id",
            "name": {$first: "$name"}, 
            "required_age": {$first: "$required_age"}, 
            "is_free": {$first: "$is_free"}, 
            "controller_support": {$first: "$controller_support"}, 
            "dlc": {$first: "$dlc"}, 
            "developers": {$first: "$developers"}, 
            "publishers": {$first: "$publishers"}, 
            "categories": {$push: "$categories"}, 
            "genres": {$first: "$genres"}, 
            "recommendations": {$first: "$recommendations"}, 
            "positive": {$first: "$positive"}, 
            "negative": {$first: "$negative"}, 
            "owners_min": {$first: "$owners_min"}, 
            "owners_max": {$first: "$owners_max"}, 
            "price": {$first: "$price"}, 
            "average_forever": {$first: "$average_forever"}, 
            "median_forever": {$first: "$median_forever"}
        }
    }
    , 
    {
        $unwind: "$genres"
    }
    ,
    {
        $project: {
            "owners_min": "$owners_min", 
            "owners_max": "$owners_max", 
            "name": "$name", 
            "required_age": "$required_age", 
            "is_free": "$is_free", 
            "controller_support": "$controller_support", 
            "dlc": "$dlc", 
            "developers": "$developers", 
            "publishers": "$publishers", 
            "categories": "$categories", 
            "genres": {$split: ["$genres", "'description': '"]}, 
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
            "owners_min": "$owners_min", 
            "owners_max": "$owners_max", 
            "name": "$name", 
            "required_age": "$required_age", 
            "is_free": "$is_free", 
            "controller_support": "$controller_support", 
            "dlc": "$dlc", 
            "developers": "$developers", 
            "publishers": "$publishers", 
            "categories": "$categories", 
            "genres": {$arrayElemAt: ["$genres", 1]}, 
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
            "owners_min": "$owners_min", 
            "owners_max": "$owners_max", 
            "name": "$name", 
            "required_age": "$required_age", 
            "is_free": "$is_free", 
            "controller_support": "$controller_support", 
            "dlc": "$dlc", 
            "developers": "$developers", 
            "publishers": "$publishers", 
            "categories": "$categories", 
            "genres": {$split: ["$genres", "'"]}, 
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
            "owners_min": "$owners_min", 
            "owners_max": "$owners_max", 
            "name": "$name", 
            "required_age": "$required_age", 
            "is_free": "$is_free", 
            "controller_support": "$controller_support", 
            "dlc": "$dlc", 
            "developers": "$developers", 
            "publishers": "$publishers", 
            "categories": "$categories", 
            "genres": {$arrayElemAt: ["$genres", 0]}, 
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
        $group: {
            "_id": "$_id",
            "name": {$first: "$name"}, 
            "required_age": {$first: "$required_age"}, 
            "is_free": {$first: "$is_free"}, 
            "controller_support": {$first: "$controller_support"}, 
            "dlc": {$first: "$dlc"}, 
            "developers": {$first: "$developers"}, 
            "publishers": {$first: "$publishers"}, 
            "categories": {$first: "$categories"}, 
            "genres": {$push: "$genres"}, 
            "recommendations": {$first: "$recommendations"}, 
            "positive": {$first: "$positive"}, 
            "negative": {$first: "$negative"}, 
            "owners_min": {$first: "$owners_min"}, 
            "owners_max": {$first: "$owners_max"}, 
            "price": {$first: "$price"}, 
            "average_forever": {$first: "$average_forever"}, 
            "median_forever": {$first: "$median_forever"}
        }
    }
    , 
    {
        $unwind: "$developers"
    }
    ,
    {
        $project: {
            "owners_min": "$owners_min", 
            "owners_max": "$owners_max", 
            "name": "$name", 
            "required_age": "$required_age", 
            "is_free": "$is_free", 
            "controller_support": "$controller_support", 
            "dlc": "$dlc", 
            "developers": {$split: ["$developers", "'"]}, 
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
            "owners_min": "$owners_min", 
            "owners_max": "$owners_max", 
            "name": "$name", 
            "required_age": "$required_age", 
            "is_free": "$is_free", 
            "controller_support": "$controller_support", 
            "dlc": "$dlc", 
            "developers": {$arrayElemAt: ["$developers", 1]}, 
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
        $group: {
            "_id": "$_id",
            "name": {$first: "$name"}, 
            "required_age": {$first: "$required_age"}, 
            "is_free": {$first: "$is_free"}, 
            "controller_support": {$first: "$controller_support"}, 
            "dlc": {$first: "$dlc"}, 
            "developers": {$push: "$developers"}, 
            "publishers": {$first: "$publishers"}, 
            "categories": {$first: "$categories"}, 
            "genres": {$first: "$genres"}, 
            "recommendations": {$first: "$recommendations"}, 
            "positive": {$first: "$positive"}, 
            "negative": {$first: "$negative"}, 
            "owners_min": {$first: "$owners_min"}, 
            "owners_max": {$first: "$owners_max"}, 
            "price": {$first: "$price"}, 
            "average_forever": {$first: "$average_forever"}, 
            "median_forever": {$first: "$median_forever"}
        }
    }
    ,
    {
        $unwind: "$publishers"
    }
    ,
    {
        $project: {
            "owners_min": "$owners_min", 
            "owners_max": "$owners_max", 
            "name": "$name", 
            "required_age": "$required_age", 
            "is_free": "$is_free", 
            "controller_support": "$controller_support", 
            "dlc": "$dlc", 
            "developers": "$developers", 
            "publishers": {$split: ["$publishers", "'"]}, 
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
            "owners_min": "$owners_min", 
            "owners_max": "$owners_max", 
            "name": "$name", 
            "required_age": "$required_age", 
            "is_free": "$is_free", 
            "controller_support": "$controller_support", 
            "dlc": "$dlc", 
            "developers": "$developers", 
            "publishers": {$arrayElemAt: ["$publishers", 1]}, 
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
        $group: {
            "_id": "$_id",
            "name": {$first: "$name"}, 
            "required_age": {$first: "$required_age"}, 
            "is_free": {$first: "$is_free"}, 
            "controller_support": {$first: "$controller_support"}, 
            "dlc": {$first: "$dlc"}, 
            "developers": {$first: "$developers"}, 
            "publishers": {$push: "$publishers"}, 
            "categories": {$first: "$categories"}, 
            "genres": {$first: "$genres"}, 
            "recommendations": {$first: "$recommendations"}, 
            "positive": {$first: "$positive"}, 
            "negative": {$first: "$negative"}, 
            "owners_min": {$first: "$owners_min"}, 
            "owners_max": {$first: "$owners_max"}, 
            "price": {$first: "$price"}, 
            "average_forever": {$first: "$average_forever"}, 
            "median_forever": {$first: "$median_forever"}
        }
    }
    ,
    {
        $project: {
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
            "spy_data.owners_min": "$owners_min", 
            "spy_data.owners_max": "$owners_max", 
            "spy_data.price": "$price", 
            "spy_data.average_forever": "$average_forever", 
            "spy_data.median_forever": "$median_forever"
        }
    }
    
    ,
    {
        $out: "steam_aggregate"
    }
]);
























