/// <reference path="../main.js" />
/*global angular:false */
'use strict';

clusterManagerApp.controller('ServersCtrl', function mainCtrl($scope, $dialog) {

    $scope.openAuthenticationDialog = function (server) {
        var dialog = $dialog.dialog({
            backdrop: true,
            keyboard: true,
            backdropClick: true,
            mode: $scope.mode,
            templateUrl: '/views/authenticationDialog.html',
            controller: 'ServerAuthenticationDialogCtrl',
            resolve: {
                getServer: function () {
                    return server;
                }
            }
        });
        dialog.open().then(function (result) {
            if (result) {
                alert('dialog closed with result: ' + result);
            }
        });
    };
});

clusterManagerApp.controller('ServerAuthenticationDialogCtrl', function mainCtrl($scope, dialog, getServer, $http) {
    $scope.server = getServer;
    if (!$scope.server.authenticationMode) {
        $scope.server.authenticationMode = 'apiKey';
    }
    $scope.close = function () {
        dialog.close();
    };

    $scope.testCredentials = function (server) {
        $http.post('/api/servers/test-credentials', server)
            .success(function(data) {
            }).error(function(data) {
                debugger
            });
    };
});