/// <reference path="../../Scripts/typings/knockout/knockout.d.ts" />

class oauthContext {

    static apiKey = ko.observable<string>(null);

    static authHeader = ko.observable<string>(null);

    static enterApiKeyTask: JQueryPromise<any>;

	static clean() {
		oauthContext.apiKey(null);
		oauthContext.authHeader(null);
	}

    static apiKeyName = ko.computed(() => {
        var key = oauthContext.apiKey();
        if (key === null) {
            return null;
        }
        var slashPos = key.indexOf('/');
        if (slashPos >= 0) {
            return key.substring(0, slashPos);
        }
        return null;
    });

    static apiKeySecret = ko.computed(() => {
        var key = oauthContext.apiKey();
        if (key === null) {
            return null;
        }
        var slashPos = key.indexOf('/');
        if (slashPos >= 0) {
            return key.substring(slashPos + 1);
        }
        return null;
    });
}

export = oauthContext;