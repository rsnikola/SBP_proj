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
            "developers": {$split: ["$developers", "["]}, 
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
            "developers": {$split: [{$arrayElemAt: ["$developers", 1]}, "]"]}, 
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
            "developers": {$arrayElemAt: ["$developers", 0]}, 
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
