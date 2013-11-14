/// <reference path="./typings/knockout/knockout.d.ts" />

interface KnockoutObservable<T> {
    where(predicate: (item: T) => boolean): KnockoutObservable<string>;
    throttle(throttleTimeInMs: number): KnockoutObservable<T>;
    select<TReturn>(selector: (item: any) => any): KnockoutObservable<TReturn>;
    distinctUntilChanged(): KnockoutObservable<T>;
}

interface KnockoutObservableArray<T> {
    pushAll(items: T[]): number;
    contains(item: T): boolean;
    first(filter?: (item) => boolean): T;
    last(filter?: (item) => boolean): T;
}

interface Function {
    memoize(thisArg: any): Function;
}

var subscribableFn: any = ko.subscribable.fn;
var observableArrayFn: any = ko.observableArray.fn;

// observable.where
subscribableFn.where = function (predicate: (item) => boolean) {
    var observable: KnockoutObservable<any> = this;
    var matches = ko.observable();
    observable.subscribe(val => {
        if (predicate(val)) {
            matches(val);
        }
    });
    return matches;
}

// observable.distinctUntilChanged
subscribableFn.distinctUntilChanged = function () {
    var observable: KnockoutObservable<any> = this;
    var matches = ko.observable();
    var lastMatch = observable();
    observable.subscribe(val => {
        if (val !== lastMatch) {
            lastMatch = val;
            matches(val);
        }
    });
    return matches;
}

// observable.throttled
subscribableFn.throttle = function (throttleTimeMs: number) {
    var observable = this;
    return ko.computed(() => observable()).extend({ throttle: throttleTimeMs });
}

// observable.select
subscribableFn.select = function (selector: (any) => any) {
    var observable = this;
    var selectedResults = ko.observable();
    observable.subscribe(val => selectedResults(selector(val)));
    return selectedResults;
}

// observableArray.pushAll
observableArrayFn.pushAll = function (items: Array<any>) {
    this.push.apply(this, items);
}

// observableArray.contains
observableArrayFn.contains = function (item: any) {
    return this.indexOf(item) !== -1;
}

// observableArray.first
observableArrayFn.first = function (filter?: (item) => boolean) {
    return this().first(filter);
}

// observableArray.last
observableArrayFn.last = function (filter?: (item) => boolean) {
    return this().last(filter);
}

// Function.memoize
var functionPrototype: any = Function.prototype;
functionPrototype.memoize = function (thisVal) {
    var self = this
    var cache = {};
    return function (arg) {
        if (arg in cache) {
            return cache[arg];
        } else {
            return cache[arg] = self.call(thisVal, arg);
        }
    }
}

// Array extensions

interface Array<T> {
    remove<T>(item: T): number;
    removeAll<T>(items: T[]): void;
    first<T>(filter?: (item: T) => boolean): T;
    last<T>(filter?: (item: T) => boolean): T;
    pushAll<T>(items: T[]): void;
    contains<T>(item: T): boolean;
}

// Array.remove
var arrayPrototype: any = Array.prototype;
arrayPrototype.remove = function (item) {
    var self: any[] = this;
    var index = self.indexOf(item);
    if (index >= 0) {
        self.splice(index, 1);
    }
    return index;
}

// Array.removeAll
arrayPrototype.removeAll = function (items: Array<any>) {
    var i = 0;
    var self: Array<any> = this;
    for (var i = self.length - 1; i >= 0 && items.length > 0; i--) {
        var itemsIndex = items.indexOf(self[i]);
        if (itemsIndex >= 0) {
            self.splice(i, 1);
            items.splice(itemsIndex);
        }
    }
}

// Array.first
arrayPrototype.first = function (filter?: (item) => boolean) {
    var self: any[] = this;
    if (self.length > 0) {
        if (filter) {
            return ko.utils.arrayFirst(self, filter);
        }
        else if (self.length > 0) {
            return self[0];
        }
    }

    return null;
}

// Array.last
arrayPrototype.last = function (filter?: (item) => boolean) {
    var self: any[] = this;
    if (filter) {
        for (var i = self.length - 1; i > 0; i--) {
            if (filter(self[i])) {
                return self[i];
            }
        }
    }
    else if (self.length > 0) {
        return self[self.length - 1];
    }

    return null;
}

// Array.pushAll
arrayPrototype.pushAll = function (items: Array<any>) {
    this.push.apply(this, items);
}

// Array.contains
arrayPrototype.contains = function (item: any) {
    var self: any[] = this;
    return self.indexOf(item) !== -1;
}