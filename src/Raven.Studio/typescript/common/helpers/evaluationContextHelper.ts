/// <reference path="../../../typings/tsd.d.ts" />

import messagePublisher = require("common/messagePublisher");


class evaluationContextHelper {

    static createContext(commonJsModule: string) {
        if (!commonJsModule) {
            return null;
        }
        try {
            const contextCreator = new Function("var exports = {}; " + commonJsModule + "; return exports;");
            return contextCreator();
        } catch (e) {
            messagePublisher.reportError("Unable to create evaluation context", e);
            return null;
        }
    }

    static wrap(contextObject: object, innerFunction: Function): Function {
        if (!contextObject) {
            return innerFunction;
        }

        return function () {
            const keys = Object.keys(contextObject);
            const propertiesToRestore = new Map<string, any>();

            try {

                keys.forEach(key => {
                    if (key in window) {
                        propertiesToRestore.set(key, (window as any)[key]);
                    }
                    (window as any)[key] = (contextObject as any)[key];
                });


// ReSharper disable once SuspiciousThisUsage
                return innerFunction.apply(this, arguments);
            } finally {
                keys.forEach(key => {
                    // this restores previous function or sets back to undefined
                    (window as any)[key] = propertiesToRestore.get(key);
                });
            }
        };
    }
}

export = evaluationContextHelper;