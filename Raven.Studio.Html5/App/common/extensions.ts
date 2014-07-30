class extensions {
    static install() {
        extensions.installArrayExtensions();
        extensions.installFunctionExtensions();
        extensions.installObservableExtensions();
        extensions.installStringExtension();
        extensions.installStorageExtension();
        extensions.installBindingHandlers();
    }

    private static installObservableExtensions() {
        var subscribableFn: any = ko.subscribable.fn;
        var observableArrayFn: any = ko.observableArray.fn;

        // observable.where
        subscribableFn.where = function (predicate: (item) => boolean) {
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
        subscribableFn.select = function (selector: (any) => any) {
            var observable = this;
            var selectedResults = ko.observable();
            observable.subscribe(val => selectedResults(selector(val)));
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
        observableArrayFn.first = function (filter?: (item) => boolean) {
            return this().first(filter);
        };

        // observableArray.last
        observableArrayFn.last = function (filter?: (item) => boolean) {
            return this().last(filter);
        };
    }

    private static installArrayExtensions() {
        // Array.remove
        var arrayPrototype: any = Array.prototype;
        arrayPrototype.remove = function (item) {
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
        };

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
        arrayPrototype.count = function (filter?: (item) => boolean) {
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

        // Array.count
        arrayPrototype.distinct = function () {
            var distinctElements = [];
            for (var i = 0; i < this.length; i++) {
                var element = this[i];
                if (!distinctElements.contains(element)) {
                    distinctElements.push(element);
                }
            }

            return distinctElements;
        };
    }

    private static installStringExtension() {
        String.prototype.fixedCharCodeAt = function (idx) {
            idx = idx || 0;
            var code = this.charCodeAt(idx);
            var hi, low;
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
                return false;
            }
            return code;
        };

        String.prototype.getSizeInBytesAsUTF8 = function () {
            var result = 0;
            for (var n = 0; n < this.length; n++) {
                var charCode = this.fixedCharCodeAt(n);
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

        String.prototype.replaceAll = function (find, replace) {
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
            return this.indexOf(str) > 0;
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
        functionPrototype.memoize = function (thisVal) {
            var self = this;
            var cache = {};
            return (arg) => {
                if (arg in cache) {
                    return cache[arg];
                } else {
                    return cache[arg] = self.call(thisVal, arg);
                }
            };
        };
    }

    private static installBindingHandlers() {
        ko.bindingHandlers['numericValue'] = {
            init: (element, valueAccessor, allBindingsAccessor, viewModel, bindingContext) => {
                var underlyingObservable = valueAccessor();
                var interceptor = ko.computed({
                    read: underlyingObservable,
                    write: value => {
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

        ko.bindingHandlers['customValidity'] = {
            update: (element, valueAccessor) => {
                var errorMessage = ko.unwrap(valueAccessor()); //unwrap to get subscription
                element.setCustomValidity(errorMessage);
            }
        };
    }
}

export = extensions;