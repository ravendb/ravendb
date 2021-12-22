// By: Hans Fjällemark and John Papa
// https://github.com/CodeSeven/KoLite
//
// Knockout.DirtyFlag
//
// John Papa
//          http://johnpapa.net
//          http://twitter.com/@john_papa
//
// Depends on scripts:
//          Knockout
//
//  Notes:
//          Special thanks to Steve Sanderson and Ryan Niemeyer for
//          their influence and help.
//
//  Usage:
//          To Setup Tracking, add this tracker property to your viewModel
//              ===> viewModel.dirtyFlag = new ko.DirtyFlag(viewModel.model);
//
//          Hook these into your view ...
//              Did It Change?
//              ===> viewModel.dirtyFlag().isDirty();
//
//          Hook this into your view model functions (ex: load, save) ...
//              Resync Changes
//              ===> viewModel.dirtyFlag().reset();
//
//          Optionally, you can pass your own hashFunction for state tracking.
//
////////////////////////////////////////////////////////////////////////////////////////

(function (factory) {
    if (typeof require === "function" && typeof exports === "object" && typeof module === "object") {
        factory(require("knockout"), exports);
    } else if (typeof define === "function" && define["amd"]) {
        define(["knockout", "exports"], factory);
    } else {
        factory(ko, ko);
    }
}(function (ko, exports) {
    if (typeof (ko) === undefined) {
        throw 'Knockout is required, please ensure it is loaded before loading the dirty flag plug-in';
    }

    exports.DirtyFlag = function (objectToTrack, isInitiallyDirty, hashFunction) {

        hashFunction = hashFunction || ko.toJSON;

        var
            self = this,
            _objectToTrack = objectToTrack,
            _lastCleanState = ko.observable(hashFunction(_objectToTrack)),
            _isInitiallyDirty = ko.observable(isInitiallyDirty),

            result = function () {
                self.forceDirty = function ()
                {
                    _isInitiallyDirty(true);
                };

                self.isDirty = ko.computed(function () {
                    return _isInitiallyDirty() || hashFunction(_objectToTrack) !== _lastCleanState();
                });

                self.reset = function () {
                    _lastCleanState(hashFunction(_objectToTrack));
                    _isInitiallyDirty(false);
                };
                return self;
            };

        return result;
    };
}));
