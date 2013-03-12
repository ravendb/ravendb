/// <reference path="../main.js" />
/*global angular:false */
'use strict';

clusterManagerApp.controller('ServersCtrl', function serverCtrl($scope, $dialog) {

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
            
        });
    };
    
    $scope.deleteServer = function (server) {
        if (confirm("Really delete the server? (" + server.url + ')')) {
            $http.delete('/api/servers/' + server.id).success(function () {
                $scope.stats.servers = $scope.stats.servers.filter(function (item) {
                    return item.id != server.id;
                });
            });
        }
    };
});

clusterManagerApp.controller('ServerAuthenticationDialogCtrl', function serverAuthenticationDialogCtrl($scope, dialog, getServer, $http) {
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

    $scope.saveCredentials = function (server) {
        $http.post('/api/servers/save-credentials', server)
            .success(function () {
                dialog.close('saved');
            });
    };
});