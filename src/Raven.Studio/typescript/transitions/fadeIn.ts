/// <reference path="../../typings/tsd.d.ts" />

declare var define: RequireDefine;

define(['durandal/system', 'transitions/transitionHelper'], function (system, helper) {
    var settings = {
        inAnimation: 'fadeIn',
        outAnimation: 'fadeOut'
    },
    fadeIn = function (context) {
        system.extend(context, settings);
        return helper.create(context);
    };

    return fadeIn;
});
