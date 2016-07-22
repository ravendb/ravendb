/// <reference path="../../../../typings/tsd.d.ts"/>

class serverConnectionInfo {

    url = ko.observable<string>();
    username = ko.observable<string>();
    password = ko.observable<string>();
    domain = ko.observable<string>();
    apiKey = ko.observable<string>();

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

    guessCredentialsType() {
        if (this.apiKey()) {
            this.useApiKeyCredentials();
        } else if (this.username()) {
            this.useUserCredentials();
        }
    }

    useUserCredentials() {
        this.isUserCredentials(true);
        this.isApiKeyCredentials(false);
        this.clearApiKeyCredentials();
    }

    useApiKeyCredentials() {
        this.isApiKeyCredentials(true);
        this.isUserCredentials(false);
        this.clearUserCredentials();
    }

    useNoCredentials() {
        this.isUserCredentials(false);
        this.isApiKeyCredentials(false);
        this.clearApiKeyCredentials();
        this.clearUserCredentials();
    }

    private clearApiKeyCredentials() {
        this.apiKey(undefined);
    }

    private clearUserCredentials() {
        this.username(undefined);
        this.password(undefined);
        this.domain(undefined);
    }

    toDto(): serverConnectionInfoDto {
        return {
            Url: this.url(),
            Username: this.username(),
            Password: this.password(),
            Domain: this.domain(),
            ApiKey: this.apiKey()
        };
    }
}

export = serverConnectionInfo;
