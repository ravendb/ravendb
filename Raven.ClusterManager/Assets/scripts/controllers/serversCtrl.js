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
                server: function () {
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

clusterManagerApp.controller('ServerAuthenticationDialogCtrl', function mainCtrl($scope, dialog, server) {
    if (!server.authenticationMode) {
        server.authenticationMode = 'apiKey';
    }
    $scope.server = server;
    $scope.close = function () {
        dialog.close();
    };
});