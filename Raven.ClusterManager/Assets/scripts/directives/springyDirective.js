/// <reference path="../main.js" />
/// <reference path="../vendor/springy/springy.js" />
/// <reference path="../vendor/springy/springyui.js" />
/*global angular:false */
'use strict';

clusterManagerApp.directive('cmSpringy', function cmSpringy() {
    return function (scope, element, attrs) {
        scope.$watch(function(scope1) {
            return scope1.$eval(attrs.cmSpringy);
        }, function(value) {
            element.springy({ graph: value });
        });
    };
});