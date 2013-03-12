/// <reference path="../main.js" />
/*global angular:false */
'use strict';

clusterManagerApp.controller('ServerExplorerCtrl', function serverExplorerCtrl($scope, $routeParams, $timeout) {
    var id = $routeParams.id;

    var getServerCountOfInvocations = 0;
    $scope.getServer = function () {
        if (!$scope.stats && getServerCountOfInvocations < 3) {
            getServerCountOfInvocations += 1;
            $timeout($scope.getServer, 1000);
            return;
        }
        
        var serverInArray = $scope.stats.servers.filter(function (item) {
            return item.id == id;
        });

        if (serverInArray.length == 1) {
            $scope.server = serverInArray[0];
        }
    };
    $scope.getServer();
});