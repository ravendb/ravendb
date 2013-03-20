/// <reference path="../main.js" />
/// <reference path="../vendor/springy/springy.js" />
/// <reference path="../vendor/springy/springyui.js" />
/*global angular:false */
'use strict';

clusterManagerApp.controller('ReplicationCtrl', function replicationCtrl($scope, $http) {
    
    var graph = new Graph();

    var timeoutPromise;
    $scope.getReplicationDatabases = function () {
        $http.get('/api/replication').success(function (result) {
            var replicationDatabases;
            $scope.replicationDatabases = replicationDatabases = result;
            
            var nodes = [];
            for (var i = 0; i < replicationDatabases.length; i++) {
                var replicationDatabase = replicationDatabases[i];
                nodes.push(graph.newNode({
                    label: replicationDatabase.serverId.replace('serverRecords/', '') + "/" + replicationDatabase.name,
                    url: replicationDatabase.serverUrl,
                    databaseUrl: replicationDatabase.databaseUrl,
                    database: replicationDatabase.name
                }));
            }

            for (var i = 0; i < replicationDatabases.length; i++) {
                var replicationDatabase = replicationDatabases[i];
                if (!replicationDatabase.isReplicationEnabled) {
                    continue;
                }

                for (var j = 0; j < replicationDatabase.replicationDestinations.length; j++) {
                    var replicationDestination = replicationDatabase.replicationDestinations[j];
                    var node1 = nodes.filter(function (item) {
                        return item.data.url.replace(/\/+$/, '') == replicationDatabase.serverUrl.replace(/\/+$/, '') &&
                               item.data.database == replicationDatabase.name;
                    })[0];
                    var node2 = nodes.filter(function (item) {
                        return item.data.databaseUrl == replicationDestination.url.replace(/\/+$/, '');
                    })[0];
                    if (!node1 || !node2) {
                        debugger
                    }
                    graph.newEdge(node1, node2, { color: '#EB6841' });
                }
            }
        });

        // timeoutPromise = $timeout($scope.getStats, 5000);
    };
    $scope.getReplicationDatabases();
    
    /*_.forEach($scope.replicationDatabases, function (value, index, array) {
        value.cssClass = '';
        if (value.isOnline) {
            if (value.isUnauthorized) {
                value.cssClass += ' warning';
            } else {
                value.cssClass += ' success';
            }
        } else {
            value.cssClass += ' error';
        }
    });*/

    $scope.graph = graph;
});