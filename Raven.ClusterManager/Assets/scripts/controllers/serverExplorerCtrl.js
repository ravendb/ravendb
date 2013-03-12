/// <reference path="../main.js" />
/*global angular:false */
'use strict';

clusterManagerApp.controller('ServerExplorerCtrl', function serverExplorerCtrl($scope, $routeParams) {
    $scope.id = $routeParams.id;
});
