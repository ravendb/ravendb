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
                if (value.isOnline) {
                    if (value.isUnauthorized) {
                        value.cssClass += ' warning';
                    } else {
                        value.cssClass += ' success';
                    }
                } else {
                    value.cssClass += ' error';
                }
            });
        });
        
        // timeoutPromise = $timeout($scope.getStats, 5000);
    };
    $scope.getStats();
});