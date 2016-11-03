import replicationPatchScript = require("models/database/replication/replicationPatchScript");
import collection = require("models/database/documents/collection");

class replicationDestination {

    url = ko.observable<string>();
    username = ko.observable<string>();
    password = ko.observable<string>();
    domain = ko.observable<string>();
    apiKey = ko.observable<string>();
    database = ko.observable<string>();
    transitiveReplicationBehavior = ko.observable<string>();
    ignoredClient = ko.observable<boolean>();
    disabled = ko.observable<boolean>();
    clientVisibleUrl = ko.observable<string>();
    skipIndexReplication = ko.observable<boolean>();

    specifiedCollections = ko.observableArray<replicationPatchScript>().extend({ required: false });
    withScripts = ko.observableArray<string>([]);
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

    toggleIsAdvancedShows(item: any, event: JQueryEventObject) {
        $(event.target).next().toggle();
    }

    constructor(dto: Raven.Abstractions.Replication.ReplicationDestination) {
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
        this.specifiedCollections(this.mapSpecifiedCollections(dto.SpecifiedCollections));
        this.withScripts(this.specifiedCollections().filter(x => typeof (x.script()) !== "undefined").map(x => x.collection()));

        this.enableReplicateOnlyFromCollections = ko.observable<boolean>(this.specifiedCollections().length > 0);

        if (this.username()) {
            this.isUserCredentials(true);
        } else if (this.apiKey()) {
            this.isApiKeyCredentials(true);
        }

        this.skipIndexReplication.subscribe(() => ko.postbox.publish('skip-index-replication'));

    }

    mapSpecifiedCollections(input: dictionary<string>) {
        var result: replicationPatchScript[] = [];

        if (!input) {
            return result;
        }
        for (var key in input) {
            if (input.hasOwnProperty(key)) {
                var item = replicationPatchScript.empty();
                item.collection(key);
                var script = input[key];
                if (script === null) {
                    script = undefined;
                }
                item.script(script);
                result.push(item);
            }
        }
        return result;
    }

    isSelected(col: collection) {
        var cols = this.specifiedCollections();
        return !!cols.first(c => c.collection() === col.name);
    }

    hasScript(col: collection) {
        var cols = this.specifiedCollections();
        var item = cols.first(c => c.collection() === col.name);
        if (!item) {
            return false;
        }
        return typeof(item.script()) !== "undefined";
    }

    toggleSkipIndexReplication() {
        this.skipIndexReplication.toggle();
    }

    static empty(databaseName: string): replicationDestination {
        return new replicationDestination({
            Url: location.protocol + "//" + location.host,
            Username: null,
            Password: null,
            Domain: null,
            ApiKey: null,
            
            Database: databaseName,
            TransitiveReplicationBehavior: "Replicate" as Raven.Abstractions.Replication.TransitiveReplicationOptions,
            IgnoredClient: false,
            Disabled: false,
            ClientVisibleUrl: null,
            SkipIndexReplication: false,
            SpecifiedCollections: {} as { [key: string]: string; }
        } as Raven.Abstractions.Replication.ReplicationDestination);
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

    toDto(): Raven.Abstractions.Replication.ReplicationDestination {
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
            SpecifiedCollections: this.enableReplicateOnlyFromCollections() ? this.specifiedCollectionsToMap() : null
        } as Raven.Abstractions.Replication.ReplicationDestination;
    }

    specifiedCollectionsToMap(): dictionary<string> {
        var result: dictionary<string> = {};
        var collections = this.specifiedCollections();
        for (var i = 0; i < collections.length; i++) {
            var item = collections[i];
            var script = item.script();
            if (!script) {
                script = null;
            }
            result[item.collection()] = script;
        }
        return result;
    }

    private prepareUrl() {
        var url = this.url();
        if (url && url.charAt(url.length - 1) === "/") {
            url = url.substring(0, url.length - 1);
        }
        return url;
    }

    addNewCollection() {
        this.specifiedCollections.push(replicationPatchScript.empty());
    }

    removeCollection(item: replicationPatchScript) {
        this.specifiedCollections.remove(item);
    }
}

export = replicationDestination;
