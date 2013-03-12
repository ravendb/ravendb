/// <reference path="../main.js" />
/// <reference path="../vendor/springy/springy.js" />
/// <reference path="../vendor/springy/springyui.js" />
/*global angular:false */
'use strict';

clusterManagerApp.controller('ReplicationCtrl', function serverCtrl($scope) {
    
    var graph = new Graph();
    
    var nodes = [];
    for (var i = 0; i < 50; i++) {
        nodes.push(graph.newNode({ label: 'DB #' + i }));
    }

    for (var i = 0; i < nodes.length; i++) {
        var index = Math.floor((Math.random() * nodes.length));
        var index2 = Math.floor((Math.random() * nodes.length));
        var node1 = nodes[index];
        var node2 = nodes[index2];
        graph.newEdge(node1, node2, { color: '#EB6841' });
    }

    $scope.graph = graph;
});