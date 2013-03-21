/// <reference path="../main.js" />
/*global angular:false */
'use strict';

clusterManagerApp.factory('nowTime', ['$timeout', function ($timeout) {
    var nowTime;
    (function updateTime() {
        nowTime = Date.now();
        $timeout(updateTime, 1000);
    }());
    return function () {
        return nowTime;
    };
}]);

clusterManagerApp.filter('timeAgo', ['nowTime', function (now) {
    return function (input) {
        return moment(input).from(now());
    };
}]);