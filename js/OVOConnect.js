(function() {
    // Create the connector object
    var myConnector = tableau.makeConnector();

    // Define the schema
    myConnector.getSchema = function(schemaCallback) {
        var daily_cols = [{
                id: "mpan",
                alias: "mpan",
                dataType: tableau.dataTypeEnum.int
            }, {
                id: "utilityType",
                alias: "utilityType",
                dataType: tableau.dataTypeEnum.string
            },
            {
                id: "consumption",
                alias: "consumption",
                dataType: tableau.dataTypeEnum.float
            }, {
                id: "price",
                alias: "price",
                dataType: tableau.dataTypeEnum.float
            }, {
                id: "standingCharge",
                alias: "standingCharge",
                dataType: tableau.dataTypeEnum.float
            }, {
                id: "startTime",
                alias: "startTime",
                dataType: tableau.dataTypeEnum.int
            }, {
                id: "formattedStartTime",
                alias: "formattedStartTime",
                dataType: tableau.dataTypeEnum.datetime
            }, {
                id: "tariff",
                alias: "tariff",
                dataType: tableau.dataTypeEnum.float
            }
        ];

        var dailyTableSchema = {
            id: "daily",
            alias: "OVO Energy Smart Meter Daily Usage",
            columns: daily_cols
        };

        var hourly_cols = [{
                id: "mpan",
                alias: "mpan",
                dataType: tableau.dataTypeEnum.int
            }, {
                id: "utilityType",
                alias: "utilityType",
                dataType: tableau.dataTypeEnum.string
            },
            {
                id: "consumption",
                alias: "consumption",
                dataType: tableau.dataTypeEnum.float
            }, {
                id: "startTime",
                alias: "startTime",
                dataType: tableau.dataTypeEnum.int
            }, {
                id: "formattedStartTime",
                alias: "formattedStartTime",
                dataType: tableau.dataTypeEnum.datetime
            }
        ];

        var hourlyTableSchema = {
            id: "hourly",
            alias: "OVO Energy Smart Meter Hourly Usage",
            columns: hourly_cols
        };

        schemaCallback([dailyTableSchema, hourlyTableSchema]);
    };

    // Download the data
    myConnector.getData = function(table, doneCallback) {
        var connectionData = JSON.parse(tableau.connectionData);
        var proxy = 'http://files.tableaujunkie.com/ovo/php/ovoproxy.php?u=';
        var url = proxy + 'https://my.ovoenergy.com/api/auth/login';
        var auth = {};
        auth.username = tableau.username;
        auth.password = tableau.password;
        auth.rememberMe = 'true'
        auth = JSON.stringify(auth);
        $.ajax({
            url: url,
            contentType: 'application/json;charset=UTF-8',
            method: 'POST',
            data: auth,
            success: function(data) {
                resp = JSON.parse(data);
                if (resp.code == 'Unknown') {
                    tableau.log("Error: " + resp.message);
                    tableau.abortWithError("Error: " + resp.message);
                    return;
                }
                persistentToken = resp.persistentToken;
                token = resp.token;

                var url = proxy + 'https://paym.ovoenergy.com/api/paym/accounts&send_cookies=1';
                var myCookie = 'persistentToken=' + persistentToken + ';token=' + token;

                $.ajax({
                    url: url,
                    xhrFields: {
                        withCredentials: true
                    },
                    contentType: 'application/json',
                    //cookie: myCookie,
                    beforeSend: function(xhr) {
                        xhr.setRequestHeader('token', token);
                        xhr.setRequestHeader('persistentToken', persistentToken);
                    },
                    method: 'GET',
                    success: function(data) {
                        resp = JSON.parse(data)[0];
                        consumers = resp.consumers;
                        var tableId = table.tableInfo.id;
                        var counter = 0;
                        getConsumptions = function() {
                            consumer = consumers[counter];
                            endDate = new Date(connectionData.endDate);
                            startDate = new Date(connectionData.startDate); //start

                            var interval = '';
                            if (tableId == 'daily') {
                                interval = 'DAY';
                            }
                            if (tableId == 'hourly') {
                                interval = 'HH';
                            }
                            var baseurl = 'https://live.ovoenergy.com/api/live/meters/' + consumer.mpan + '/consumptions/aggregated?from=' + startDate.toISOString() + '&to=' + endDate.toISOString() + '&granularity=' + interval;
                            var url = proxy + encodeURIComponent(baseurl) + '&send_cookies=1'

                            $.ajax({
                                url: url,
                                xhrFields: {
                                    withCredentials: true
                                },
                                contentType: 'application/json',
                                beforeSend: function(xhr) {
                                    xhr.setRequestHeader('token', token);
                                    xhr.setRequestHeader('persistentToken', persistentToken);
                                },
                                method: 'GET',
                                success: function(data) {
                                    resp = JSON.parse(data);
                                    tableData = [];

                                    // Iterate over the JSON object
                                    for (var i = 0, len = resp.consumptions.length; i < len; i++) {
                                        tableData.push({
                                            "mpan": consumer.mpan,
                                            "utilityType": consumer.utilityType,
                                            "consumption": resp.consumptions[i].consumption,
                                            "price": resp.consumptions[i].price,
                                            "standingCharge": resp.consumptions[i].standingCharge,
                                            "startTime": resp.consumptions[i].startTime,
                                            "formattedStartTime": new Date(resp.consumptions[i].startTime),
                                            "tariff": resp.consumptions[i].tariff
                                        });
                                    }

                                    table.appendRows(tableData);
                                    counter++;
                                    if (counter == 2) {
                                        doneCallback();
                                    } else {
                                        getConsumptions(counter);
                                    }
                                }
                            })
                        }
                        getConsumptions();
                    }
                });
            }
        });
    };

    tableau.registerConnector(myConnector);


    // Create event listeners for when the user submits the form
    $(document).ready(function() {
        $("#submitButton").click(function() {
            var connectionData = {};
            tableau.password = document.getElementById('password').value;
            tableau.username = document.getElementById('username').value
            connectionData.endDate = document.getElementById("endDate").value;
            connectionData.startDate = document.getElementById("startDate").value;
            tableau.connectionData = JSON.stringify(connectionData);
            tableau.connectionName = "OVO Energy Feed for " + tableau.username; // This will be the data source name in Tableau
            tableau.submit(); // This sends the connector object to Tableau
        });
    });
    // Init function for connector, called during every phase
    myConnector.init = function(initCallback) {

        tableau.authType = tableau.authTypeEnum.custom;
        
        initCallback();
    }
})();
