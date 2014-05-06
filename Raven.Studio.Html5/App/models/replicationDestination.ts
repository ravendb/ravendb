class replicationDestination {

    url = ko.observable<string>().extend({ required: true });
    username = ko.observable<string>().extend({ required: true });
    password = ko.observable<string>().extend({ required: true });
    domain = ko.observable<string>().extend({ required: true });
    apiKey = ko.observable<string>().extend({ required: true });
    database = ko.observable<string>().extend({ required: true });
    transitiveReplicationBehavior = ko.observable<string>().extend({ required: true });
    ignoredClient = ko.observable<boolean>().extend({ required: true });
    disabled = ko.observable<boolean>().extend({ required: true });
    clientVisibleUrl = ko.observable<string>().extend({ required: true });

    name = ko.computed(() => {
        if (this.url() && this.database()) {
            return this.database() + " on " + this.url();
        } else if (this.url()) {
            return this.url();
        } else if (this.database()) {
            return this.database();
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

    toggleIsAdvancedShows(item, event) {
        $(event.target).next().toggle();
    }

    constructor(dto: replicationDestinationDto) {
        this.url(dto.Url);
        this.username(dto.Username);
        this.password(dto.Password);
        this.domain(dto.Domain);
        this.apiKey(dto.ApiKey);
        this.database(dto.Database);
        this.transitiveReplicationBehavior(dto.TransitiveReplicationBehavior);
        this.ignoredClient(dto.IgnoredClient);
        this.disabled(dto.Disabled);
        this.clientVisibleUrl(dto.ClientVisibleUrl);

        if (this.username()) {
            this.isUserCredentials(true);
        } else if (this.apiKey()) {
            this.isApiKeyCredentials(true);
        }
    }

    static empty(): replicationDestination {
        return new replicationDestination({
            Url: null,
            Username: null,
            Password: null,
            Domain: null,
            ApiKey: null,
            Database: null,
            TransitiveReplicationBehavior: "None",
            IgnoredClient: false,
            Disabled: false,
            ClientVisibleUrl: null
        });
    }

    enable() {
        this.disabled(false);
    }

    disable() {
        this.disabled(true);
    }

    includeFailover() {
        this.ignoredClient(false);
    }

    skipFailover() {
        this.ignoredClient(true);
    }

    toDto(): replicationDestinationDto {
        return {
            Url: this.prepareUrl(),
            Username: this.username(),
            Password: this.password(),
            Domain: this.domain(),
            ApiKey: this.apiKey(),
            Database: this.database(),
            TransitiveReplicationBehavior: this.transitiveReplicationBehavior(),
            IgnoredClient: this.ignoredClient(),
            Disabled: this.disabled(),
            ClientVisibleUrl: this.clientVisibleUrl()
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

export = replicationDestination;