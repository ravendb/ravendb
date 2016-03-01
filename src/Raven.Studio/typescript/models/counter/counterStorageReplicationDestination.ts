/// <reference path="../../../typings/tsd.d.ts"/>

class counterStorageReplicationDestination {

    disabled = ko.observable<boolean>().extend({ required: true });
    serverUrl = ko.observable<string>().extend({ required: true });
    counterStorageName = ko.observable<string>().extend({ required: true });
    username = ko.observable<string>().extend({ required: true });
    password = ko.observable<string>().extend({ required: true });
    domain = ko.observable<string>().extend({ required: true });
    apiKey = ko.observable<string>().extend({ required: true });

    constructor(dto: counterStorageReplicationDestinatinosDto) {
        this.disabled(dto.Disabled);
        this.serverUrl(dto.ServerUrl);
        this.counterStorageName(dto.CounterStorageName);
        this.username(dto.Username);
        this.password(dto.Password);
        this.domain(dto.Domain);
        this.apiKey(dto.ApiKey);

        if (this.username()) {
            this.isUserCredentials(true);
        } else if (this.apiKey()) {
            this.isApiKeyCredentials(true);
        }
    }

    name = ko.computed(() => {
        if (this.serverUrl() && this.counterStorageName()) {
            return this.counterStorageName() + " on " + this.serverUrl();
        } else if (this.serverUrl()) {
            return this.serverUrl();
        } else if (this.counterStorageName()) {
            return this.counterStorageName();
        }

        return "[empty]";
    });

    isValid = ko.computed(() => this.serverUrl() != null && this.serverUrl().length > 0);

    // data members for the ui
    isUserCredentials = ko.observable<boolean>(false);
    isApiKeyCredentials = ko.observable<boolean>(false);
    credentialsType = ko.computed(() => {
        if (this.isUserCredentials()) {
            return "user";
        } else if (this.isApiKeyCredentials()) {
            return "api-key";
        } else {
            return "none";
        }
    });

    toggleUserCredentials() {
        this.isUserCredentials.toggle();
        if (this.isUserCredentials()) {
            this.isApiKeyCredentials(false);
        }
    }

    toggleApiKeyCredentials() {
        this.isApiKeyCredentials.toggle();
        if (this.isApiKeyCredentials()) {
            this.isUserCredentials(false);
        }
    }

    static empty(counterStorageName: string): counterStorageReplicationDestination {
        return new counterStorageReplicationDestination({
            Disabled: false,
            ServerUrl: null,
            CounterStorageName: counterStorageName,
            Username: null,
            Password: null,
            Domain: null,
            ApiKey: null
        });
    }

    enable() {
        this.disabled(false);
    }

    disable() {
        this.disabled(true);
    }

    toDto(): counterStorageReplicationDestinatinosDto {
        return {
            Disabled: this.disabled(),
            ServerUrl: this.prepareUrl(),
            CounterStorageName: this.counterStorageName(),
            Username: this.username(),
            Password: this.password(),
            Domain: this.domain(),
            ApiKey: this.apiKey(),
        };
    }

    private prepareUrl() {
        var url = this.serverUrl();
        if (url && url.charAt(url.length - 1) === "/") {
            url = url.substring(0, url.length - 1);
        }
        return url;
    }
}

export = counterStorageReplicationDestination;
