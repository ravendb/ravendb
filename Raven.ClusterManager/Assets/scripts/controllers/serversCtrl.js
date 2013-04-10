/// <reference path="../main.js" />
/*global angular:false */
'use strict';

clusterManagerApp.controller('ServersCtrl', function serverCtrl($scope, $dialog) {

    $scope.openAuthenticationDialog = function(server) {
        //if ($scope.stats.credentials.length > 0) {
            /*var dialog1 = $dialog.dialog({
                backdrop: true,
                keyboard: true,
                backdropClick: true,
                templateUrl: '/views/linkServerWithCredentials.html',
                controller: 'LinkServerWithCredentialsCtrl',
                resolve: {
                    getServer: function() {
                        return server;
                    }
                }
            });
            dialog1.open().then(function(result) {

            });*/
       // } else {
            var dialog2 = $dialog.dialog({
                backdrop: true,
                keyboard: true,
                backdropClick: true,
                templateUrl: '/views/authenticationDialog.html',
                controller: 'ServerAuthenticationDialogCtrl',
                resolve: {
                    bridge: function () {
                        var credentials = {
                            authenticationMode: 'apiKey'
                        };
                        if (server.credentialsId) {
                            var items = $scope.stats.credentials.filter(function(item) {
                                return item.id == server.credentialsId;
                            });
                            if (items.length == 1) {
                                credentials = items[0];
                            }
                        }
                        
                        return {
                            server: server,
                            credentials: credentials
                        };
                    }
                }
            });
            dialog2.open().then(function(result) {

            });
        //}
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

clusterManagerApp.controller('ServerAuthenticationDialogCtrl', function serverAuthenticationDialogCtrl($scope, dialog, bridge, $http) {
    $scope.server = bridge.server;
    $scope.credentials = bridge.credentials;
    
    if (!$scope.credentials.authenticationMode) {
        $scope.server.authenticationMode = 'apiKey';
    }
    $scope.close = function () {
        dialog.close();
    };

    $scope.closeAlert = function (index) {
        $scope.alert = null;
    };
    
    $scope.testCredentials = function (server) {
        $scope.alert = { message: 'Testing credentials...' };
        
        $http.post('/api/servers/credentials/test?serverId=' + server.id, $scope.credentials)
            .success(function (data) {
                if (data.success) {
                    $scope.alert = { type: 'success', message: 'Credentials are good. You can save them.' };
                } else {
                    $scope.alert = { type: 'error', message: data.message };
                }
            }).error(function(data) {
                $scope.alert = { type: 'error', message: 'There was an error.' };
            });
    };

    $scope.saveCredentials = function (server) {
        $http.post('/api/servers/credentials/save?serverId=' + server.id, $scope.credentials)
            .success(function () {
                dialog.close('saved');
            });
    };
});

clusterManagerApp.controller('LinkServerWithCredentialsCtrl', function serverAuthenticationDialogCtrl($scope, dialog, getServer, $http) {
   
});