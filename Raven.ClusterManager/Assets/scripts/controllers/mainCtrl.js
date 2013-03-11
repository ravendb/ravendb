/// <reference path="../main.js" />
/*global angular:false */
'use strict';

clusterManagerApp.controller('MainCtrl', function mainCtrl($scope, $http, $timeout) {
    $scope.startDiscovering = function () {
        $scope.isDiscovering = true;
        $http.get('/api/discovery/start').success(function () {
            $scope.isDiscovering = false;
        });
    };

    var timeoutPromise;
    $scope.getStats = function () {
        $http.get('/api/servers').success(function (result) {
            $scope.stats = result;
            _.forEach($scope.stats.servers, function(value, index, array) {
                value.cssClass = '';
                if (value.isOnline == false) {
                    value.cssClass += ' error';
                }
                else if (value.isUnauthorized) {
                    value.cssClass += ' warning';
                }
            });
        });
        
        // timeoutPromise = $timeout($scope.getStats, 5000);
    };
    $scope.getStats();

    $scope.deleteServer = function (serverId) {
        $http.delete('/api/servers/' + serverId).success(function () {
            $scope.stats.servers = $scope.stats.servers.filter(function(item) {
                return item.id != serverId;
            });
        });
    };
});