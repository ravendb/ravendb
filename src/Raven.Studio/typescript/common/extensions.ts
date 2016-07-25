/// <reference path="../../typings/tsd.d.ts"/>

import moment = require("moment");

class extensions {
    static install() {
        extensions.installArrayExtensions();
        extensions.installDateExtensions();
        extensions.installFunctionExtensions();
        extensions.installObservableExtensions();
        extensions.installStringExtension();
        extensions.installStorageExtension();
        extensions.installBindingHandlers();
        extensions.installJqueryExtensions();

        // Want Intellisense for your extensions?
        // Go to extensionInterfaces.ts and add the function signature there.
    }

    private static installDateExtensions() {
        var datePrototype: any = Date.prototype;

        var formatNumber = (num: number) => {
            return num < 10 ? "0" + num : num;
        }

        datePrototype.getUTCDateFormatted = function () {
            var date = this.getUTCDate();
            return formatNumber(date);
        }

        datePrototype.getUTCMonthFormatted = function () {
            var month = this.getUTCMonth() + 1;
            return formatNumber(month);
        }

        datePrototype.getUTCHoursFormatted = function () {
            var hours = this.getUTCHours();
            return formatNumber(hours);
        }

        datePrototype.getUTCMinutesFormatted = function () {
            var minutes = this.getUTCMinutes();
            return formatNumber(minutes);
        }

        datePrototype.getUTCSecondsFormatted = function () {
            var seconds = this.getUTCSeconds();
            return formatNumber(seconds);
        }
    }

    private static installObservableExtensions() {
        var subscribableFn: any = ko.subscribable.fn;
        var observableArrayFn: any = ko.observableArray.fn;

        // observable.where
        subscribableFn.where = function (predicate: (item: any) => boolean) {
            var observable: KnockoutSubscribable<any> = this;
            var matches = ko.observable();
            observable.subscribe(val => {
                if (predicate(val)) {
                    matches(val);
                }
            });
            return matches;
        };

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
        };

        // observable.throttled
        subscribableFn.throttle = function (throttleTimeMs: number) {
            var observable = this;
            return ko.computed(() => observable()).extend({ throttle: throttleTimeMs });
        };

        // observable.select
        subscribableFn.select = function (selector: (any: any) => any) {
            var observable = this;
            var selectedResults = ko.observable();
            observable.subscribe((val:any) => selectedResults(selector(val)));
            return selectedResults;
        };

        // observable.toggle
        subscribableFn.toggle = function () {
            var observable: KnockoutObservable<boolean> = this;
            observable(!observable());
            return observable;
        };

        // observableArray.pushAll
        observableArrayFn.pushAll = function (items: Array<any>) {
            this.push.apply(this, items);
        };

        // observableArray.contains
        observableArrayFn.contains = function (item: any) {
            return this.indexOf(item) !== -1;
        };

        // observableArray.first
        observableArrayFn.first = function (filter?: (item: any) => boolean) {
            return this().first(filter);
        };

        // observableArray.last
        observableArrayFn.last = function (filter?: (item: any) => boolean) {
            return this().last(filter);
        };
    }

    private static installArrayExtensions() {
        var arrayPrototype: any = Array.prototype;

        // Array.remove
        arrayPrototype.remove = function (item: any) {
            var self: any[] = this;
            var index = self.indexOf(item);
            if (index >= 0) {
                self.splice(index, 1);
            }
            return index;
        };

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
        };

        // Array.first
        arrayPrototype.first = function (filter?: (item: any) => boolean) {
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
        };

        // Array.last
        arrayPrototype.last = function (filter?: (item: any) => boolean) {
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
        };

        // Array.pushAll
        arrayPrototype.pushAll = function (items: Array<any>) {
            this.push.apply(this, items);
        };

        // Array.contains
        arrayPrototype.contains = function (item: any) {
            var self: any[] = this;
            return self.indexOf(item) !== -1;
        };

        // Array.count
        arrayPrototype.count = function (filter?: (item: any) => boolean) {
            var self: any[] = this;
            if (filter) {
                var matches = 0;
                for (var i = 0; i < self.length; i++) {
                    if (filter(self[i])) {
                        matches++;
                    }
                }

                return matches;
            }

            return self.length;
        };

        // Array.distinct
        arrayPrototype.distinct = function () {
            var distinctElements: Array<any> = [];
            for (var i = 0; i < this.length; i++) {
                var element = this[i];
                if (!distinctElements.contains(element)) {
                    distinctElements.push(element);
                }
            }

            return distinctElements;
        };

        // Array.distinct
        arrayPrototype.concatUnique = function (values: Array<any>) {
            for (var i = 0; i < values.length; i++)
                if (this.indexOf(values[i]) === -1)
                    this.push(values[i]);
        };
    }

    private static installStringExtension() {

        String.prototype.fixedCharCodeAt = function (idx: number) {
            idx = idx || 0;
            var code = this.charCodeAt(idx);
            var hi: number, low: number;
            if (0xD800 <= code && code <= 0xDBFF) { // High surrogate (could change last hex to 0xDB7F to treat high private surrogates as single characters)
                hi = code;
                low = this.charCodeAt(idx + 1);
                if (isNaN(low)) {
                    throw 'No valid character or memory error!';
                }
                return ((hi - 0xD800) * 0x400) + (low - 0xDC00) + 0x10000;
            }
            if (0xDC00 <= code && code <= 0xDFFF) { // Low surrogate
                // We return false to allow loops to skip this iteration since should have already handled high surrogate above in the previous iteration
                return 0;
            }
            return code;
        };

        String.prototype.getSizeInBytesAsUTF8 = function () {
            var result = 0;
            var isQuoted = false;
            var prevChar : any = 0;
            for (var n = 0; n < this.length; n++) {

                var charCode = this.fixedCharCodeAt(n);

                if (charCode === 34 /*quates*/) {
                    if (!(isQuoted === true && prevChar === 92 /*backslash*/)) {
                        isQuoted = !isQuoted;
                    }
                }

                prevChar = charCode;

                // whiteSpaceCharacters list from : https://en.wikipedia.org/wiki/Whitespace_character
                var whiteSpaceCharacters = [9, 10, 11, 12, 13, 32, 133, 160, 5760, 8192, 8193, 8194, 8195, 8196, 8197, 8198, 8199, 8200, 8201, 8202, 8232, 8233, 8239, 8287, 12288, 6158, 8203, 8204, 8205, 8288, 65279];
                if (isQuoted === false && $.inArray(charCode, whiteSpaceCharacters) > -1) {
                    continue;
                }

                if (typeof charCode === "number") {
                    if (charCode < 128) {
                        result = result + 1;
                    } else if (charCode < 2048) {
                        result = result + 2;
                    } else if (charCode < 65536) {
                        result = result + 3;
                    } else if (charCode < 2097152) {
                        result = result + 4;
                    } else if (charCode < 67108864) {
                        result = result + 5;
                    } else {
                        result = result + 6;
                    }
                }
            }
            return result;
        };

        String.prototype.hashCode = function () {
            var hash = 0;
            if (this.length == 0) return hash;
            for (var i = 0; i < this.length; i++) {
                var char = this.charCodeAt(i);
                hash = ((hash << 5) - hash) + char;
                hash |= 0; // Convert to 32bit integer
            }
            return hash;
        };

        String.prototype.replaceAll = function (find: string, replace: string) {
            return this.replace(new RegExp(find, 'g'), replace);
        };

        String.prototype.reverse = (input: string) => {
            var chars = new Array;
            if (input != null) {
                chars = input.split('');
            }
            return chars.reverse().join('');
        };

        String.prototype.count = function (regex: string) {
            var re = new RegExp(regex, 'g');
            var results = this.match(re);

            return results ? results.length : 0;
        }

        String.prototype.startsWith = String.prototype.startsWith || function (str) {
            return this.indexOf(str) == 0;
        };

        String.prototype.contains = String.prototype.contains || function (str) {
            return this.indexOf(str) > -1;
        }

        String.prototype.multiply = function (amount: number) {
            var returnedString: string = new Array(amount + 1).join(this);
            return returnedString;
        }

        String.prototype.paddingLeft = function (paddingChar: string, paddingLength: number) {
            var paddingString = paddingChar.multiply(paddingLength);
            return String(paddingString + this).slice(-paddingString.length);
        }

        String.prototype.paddingRight = function (paddingChar: string, paddingLength: number) {
            var paddingString = paddingChar.multiply(paddingLength);
            return String(paddingString + this).slice(paddingString.length);
        }

        String.prototype.toHumanizedDate = function () {
            var dateMoment = moment(this.toString());
            if (dateMoment.isValid()) {
                var now = moment();
                var agoInMs = dateMoment.diff(now);
                return moment.duration(agoInMs).humanize(true) + dateMoment.format(" (MM/DD/YY, h:mma)");
            }

            return this;
        }

        String.prototype.capitalizeFirstLetter = function() {
            return this.charAt(0).toUpperCase() + this.slice(1);
        }
    }

    private static installStorageExtension() {
        Storage.prototype.getObject = function (key) {
            var value = this.getItem(key);
            return value && JSON.parse(value);
        }

        Storage.prototype.setObject = function (key, value) {
            this.setItem(key, ko.toJSON(value));
        }
    }

    private static installFunctionExtensions() {
        // Function.memoize
        var functionPrototype: any = Function.prototype;
        functionPrototype.memoize = function (thisVal: any) {
            var self = this;
            var cache: any = {};
            return (arg1: any, arg2: any) => {
                if (arg2 in cache) {
                    return cache[arg2];
                } else {
                    return cache[arg2] = self.call(thisVal, arg1, arg2);
                }
            };
        };
    }

    private static installBindingHandlers() {
        ko.bindingHandlers["numericValue"] = {
            init: (element, valueAccessor, allBindingsAccessor, viewModel, bindingContext) => {
                var underlyingObservable = valueAccessor();
                var interceptor = ko.computed({
                    read: underlyingObservable,
                    write: (value: any) => {
                        if (!isNaN(value)) {
                            underlyingObservable(parseFloat(value));
                        }
                    },
                    disposeWhenNodeIsRemoved: element
                });
                ko.bindingHandlers.value.init(element, () => interceptor, allBindingsAccessor, viewModel, bindingContext);
            },
            update: ko.bindingHandlers.value.update
        };

        ko.bindingHandlers["customValidity"] = {
            update: (element, valueAccessor) => {
                var errorMessage = ko.unwrap(valueAccessor()); //unwrap to get subscription
                element.setCustomValidity(errorMessage);
            }
        };

        ko.bindingHandlers["bsChecked"] = {
            init: (element, valueAccessor, allBindingsAccessor, viewModel, bindingContext) => {
                var value = valueAccessor();
                var newValueAccessor = () => {
                    return {
                        change() {
                            value(element.value);
                        }
                    }
                };
                if ($(element).val() === ko.unwrap(valueAccessor())) {
                    $(element).closest(".btn").button("toggle");
                }
                ko.bindingHandlers.event.init(element, newValueAccessor, allBindingsAccessor, viewModel, bindingContext);
            }
        };

        var key = "_my_init_key_";
        ko.bindingHandlers["updateHighlighting"] = {
            init: (element, valueAccessor, allBindingsAccessor, viewModel, bindingContext) => {
                ko.utils.domData.set(element, key, true);
            },
            update(element, valueAccessor, allBindings, viewModel, bindingContext) {
                var value = valueAccessor();
                var isInit = ko.utils.domData.get(element, key);
                var data = ko.dataFor(element);
                var skip = !!data.isAllDocuments || !!data.isSystemDocuments || !!data.isAllGroupsGroup;
                if (skip === false && isInit === false) {
                    $($(element).parents("li")).highlight();
                } else {
                    ko.utils.domData.set(element, key, false);
                }
            }
        };

        ko.bindingHandlers["checkboxTriple"] = {
            update(element, valueAccessor, allBindings, viewModel, bindingContext) {
                var checkboxValue: checkbox = ko.unwrap(valueAccessor());
                switch (checkboxValue) {
                case checkbox.Checked:
                    element.checked = true;
                    element.readOnly = false;
                    element.indeterminate = false;
                    break;
                case checkbox.SomeChecked:
                    element.readOnly = true;
                    element.indeterminate = true;
                    element.checked = false;
                    break;
                case checkbox.UnChecked:
                    element.checked = false;
                    element.readOnly = false;
                    element.indeterminate = false;
                    break;
                }
            }
        };
    }

    static installJqueryExtensions() {
        jQuery.fn.highlight = function() {
            $(this).each(function() {
                var el = $(this);
                el.before("<div/>");
                el.prev()
                    .width(el.width())
                    .height(el.height())
                    .css({
                        "position": "absolute",
                        "background-color": "#ffff99",
                        "opacity": ".9"
                    })
                    .fadeOut(1500);
            });
        }
    }
}

export = extensions;
