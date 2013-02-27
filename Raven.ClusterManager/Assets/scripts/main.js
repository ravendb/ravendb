/// <reference path="vendor/angular/angular.js" />
/*global angular:false */
'use strict';

var clusterManagerApp = angular.module('ClusterManagerApp', []);

clusterManagerApp.config(function ($routeProvider, $locationProvider) {
    $routeProvider.when('/', {
        templateUrl: '/views/main.html',
        controller: 'MainCtrl'
    });
    $routeProvider.otherwise({ redirectTo: '/' });
    
    $locationProvider.html5Mode(true);
});
