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
    skipIndexReplication = ko.observable<boolean>().extend({ required: true });
    hasGlobal = ko.observable<boolean>(false);
    hasLocal = ko.observable<boolean>(true);

    globalConfiguration = ko.observable<replicationDestination>();

    sourceCollections = ko.observableArray<string>().extend({required: false});
    enableReplicateOnlyFromCollections = ko.observable<boolean>();

    name = ko.computed(() => {
        var prefix = this.disabled() ? "[disabled]" : null;
        var database = this.database();
        var on = this.database() && this.url() ? "on" : null;
        var url = this.url();

        return [prefix, database, on, url]
            .filter(s => !!s)
            .join(" ")
            || "[empty]";
    });
    isValid = ko.computed(() => this.url() != null && this.url().length > 0);

    canEdit = ko.computed(() => this.hasLocal());


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

	clearApiKeyCredentials() {
		this.apiKey(null);
	}

	clearUserCredentials() {
		this.username(null);
		this.password(null);
		this.domain(null);
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
		this.clearUserCredentials();
	    this.clearApiKeyCredentials();
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
        this.skipIndexReplication(dto.SkipIndexReplication);
        this.sourceCollections(dto.SourceCollections);

        this.enableReplicateOnlyFromCollections = ko.observable<boolean>(typeof dto.SourceCollections !== 'undefined' && dto.SourceCollections.length > 0);

        if (this.username()) {
            this.isUserCredentials(true);
        } else if (this.apiKey()) {
            this.isApiKeyCredentials(true);
        }

        this.hasGlobal(dto.HasGlobal);
        this.hasLocal(dto.HasLocal);
    }

    static empty(databaseName: string): replicationDestination {
        return new replicationDestination({
            Url: null,
            Username: null,
            Password: null,
            Domain: null,
            ApiKey: null,
            Database: databaseName,
            TransitiveReplicationBehavior: "Replicate",
            IgnoredClient: false,
            Disabled: false,
            ClientVisibleUrl: null,
            SkipIndexReplication: false,
            SourceCollections: [],
            HasGlobal: false,
            HasLocal: true
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
            ClientVisibleUrl: this.clientVisibleUrl(),
            SkipIndexReplication: this.skipIndexReplication(),
			SourceCollections: this.enableReplicateOnlyFromCollections()? this.sourceCollections(): []
        };
    }

    private prepareUrl() {
        var url = this.url();
        if (url && url.charAt(url.length - 1) === "/") {
            url = url.substring(0, url.length - 1);
        }
        return url;
    }

    copyFromGlobal() {
        if (this.globalConfiguration()) {
            var gConfig = this.globalConfiguration();
            this.url(gConfig.url());
            this.username(gConfig.username());
            this.password(gConfig.password());
            this.domain(gConfig.domain());
            this.apiKey(gConfig.apiKey());
            this.database(gConfig.database());
            this.transitiveReplicationBehavior(gConfig.transitiveReplicationBehavior());
            this.ignoredClient(gConfig.ignoredClient());
            this.disabled(gConfig.disabled());
            this.clientVisibleUrl(gConfig.clientVisibleUrl());
            this.skipIndexReplication(gConfig.skipIndexReplication());
            this.hasGlobal(true);
            this.hasLocal(false);
            this.isUserCredentials(gConfig.isUserCredentials());
            this.isApiKeyCredentials(gConfig.isApiKeyCredentials());
        }
    }
}

export = replicationDestination;