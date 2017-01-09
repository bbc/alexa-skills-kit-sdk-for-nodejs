'use strict';
var aws = require('aws-sdk');
var doc;

module.exports = (function() {
    return {
        get: function(endpointURL ,table, userId, callback) {
            if(!table) {
                callback('DynamoDB Table name is not set.', null);
            }

            if(!doc) {
                if(endpointURL) {
                doc = new aws.DynamoDB.DocumentClient({apiVersion: '2012-08-10', endpoint : new aws.Endpoint(endpointURL)});
                } else {
                doc = new aws.DynamoDB.DocumentClient({apiVersion: '2012-08-10'});
                }
            }

            var params = {
                Key: {
                    userId: userId
                },
                TableName: table,
                ConsistentRead: true
            };

            doc.get(params, function(err, data){
                if(err) {
                    console.log('get error: ' + JSON.stringify(err, null, 4));

                    if(err.code === 'ResourceNotFoundException') {

                        if(endpointURL) {
                            var dynamoClient = new aws.DynamoDB({endpoint : new aws.Endpoint(endpointURL)});
                        } else {
                            var dynamoClient = new aws.DynamoDB();
                        }
                        newTableParams['TableName'] = table;
                        dynamoClient.createTable(newTableParams, function (err, data) {
                            if(err) {
                                console.log('Error creating table: ' + JSON.stringify(err, null, 4));
                            }
                            console.log('Creating table ' + table + ':\n' + JSON.stringify(data));
                            callback(err, {});
                        });
                    } else {
                        callback(err, null);
                    }
                } else {
                    if(isEmptyObject(data)) {
                        callback(null, {});
                    } else {
                        callback(null, data.Item['mapAttr']);
                    }
                }
            });
        },

        set: function(endpointURL, table, userId, data, callback) {
            if(!table) {
                callback('DynamoDB Table name is not set.', null);
            }

            if(!doc) {
                if(endpointURL) {
                    doc = new aws.DynamoDB.DocumentClient({apiVersion: '2012-08-10', endpoint : new aws.Endpoint(endpointURL)});
                } else {
                    doc = new aws.DynamoDB.DocumentClient({apiVersion: '2012-08-10'});
                }
            }

            var params = {
                Item: {
                    userId: userId,
                    mapAttr: data
                },
                TableName: table
            };

            doc.put(params, function(err, data) {
                if(err) {
                    console.log('Error during DynamoDB put:' + err);
                }
                callback(err, data);
            });
        }
    };
})();

function isEmptyObject(obj) {
    return !Object.keys(obj).length;
}

var newTableParams = {
    AttributeDefinitions: [
        {
            AttributeName: 'userId',
            AttributeType: 'S'
        }
    ],
    KeySchema: [
        {
            AttributeName: 'userId',
            KeyType: 'HASH'
        }
    ],
    ProvisionedThroughput: {
        ReadCapacityUnits: 5,
        WriteCapacityUnits: 5
    }
};
