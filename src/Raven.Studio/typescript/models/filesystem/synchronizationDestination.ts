/// <reference path="../../../typings/tsd.d.ts"/>

class synchronizationDestination {

    url = ko.observable<string>().extend({ required: true });
    username = ko.observable<string>().extend({ required: true });
    password = ko.observable<string>().extend({ required: true });
    domain = ko.observable<string>().extend({ required: true });
    apiKey = ko.observable<string>().extend({ required: true });
    filesystem = ko.observable<string>().extend({ required: true });
    disabled = ko.observable<boolean>().extend({ required: true });

    name = ko.computed(() => {
        if (this.url() && this.filesystem()) {
            return this.filesystem() + " on " + this.url();
        } else if (this.url()) {
            return this.url();
        } else if (this.filesystem()) {
            return this.filesystem();
        }

        return "[empty]";
    });

    isValid = ko.computed(() => this.url() != null && this.url().length > 0);

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

    constructor(dto: synchronizationDestinationDto) {
        this.url(dto.ServerUrl);
        this.username(dto.Username);
        this.password(dto.Password);
        this.domain(dto.Domain);
        this.apiKey(dto.ApiKey);
        this.disabled(!dto.Enabled);
        this.filesystem(dto.FileSystem);

        if (this.username()) {
            this.isUserCredentials(true);
        } else if (this.apiKey()) {
            this.isApiKeyCredentials(true);
        }
    }

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

    static empty(): synchronizationDestination {
        return new synchronizationDestination({
            ServerUrl: null,
            Username: null,
            Password: null,
            Domain: null,
            ApiKey: null,
            FileSystem: null,
            Enabled: false,
        });
    }

    enable() {
        this.disabled(false);
    }

    disable() {
        this.disabled(true);
    }

    toDto(): synchronizationDestinationDto {
        return {
            ServerUrl: this.prepareUrl(),
            Username: this.username(),
            Password: this.password(),
            Domain: this.domain(),
            ApiKey: this.apiKey(),
            FileSystem: this.filesystem(),
            Enabled: !this.disabled(),
        };
    }

    private prepareUrl() {
        var url = this.url();
        if (url && url.charAt(url.length - 1) === "/") {
            url = url.substring(0, url.length - 1);
        }
        return url;
    }
}

export = synchronizationDestination;
