import replicationPatchScript = require("models/database/replication/replicationPatchScript");
import collection = require("models/database/documents/collection");

class replicationDestination {
    /* TODO
    url = ko.observable<string>();
    database = ko.observable<string>();
    ignoredClient = ko.observable<boolean>();
    disabled = ko.observable<boolean>();
    clientVisibleUrl = ko.observable<string>();

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


    toggleIsAdvancedShows(item: any, event: JQueryEventObject) {
        $(event.target).next().toggle();
    }

    constructor(dto: Raven.Client.Documents.Replication.ReplicationDestination) {
        this.url(dto.Url);
        this.database(dto.Database);
        this.ignoredClient(dto.IgnoredClient);
        this.disabled(dto.Disabled);
        this.clientVisibleUrl(dto.ClientVisibleUrl);
        this.specifiedCollections(this.mapSpecifiedCollections(dto.SpecifiedCollections));
        this.withScripts(this.specifiedCollections().filter(x => typeof (x.script()) !== "undefined").map(x => x.collection()));

        this.enableReplicateOnlyFromCollections = ko.observable<boolean>(this.specifiedCollections().length > 0);
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
        return !!cols.find(c => c.collection() === col.name);
    }

    hasScript(col: collection) {
        var cols = this.specifiedCollections();
        var item = cols.find(c => c.collection() === col.name);
        if (!item) {
            return false;
        }
        return typeof(item.script()) !== "undefined";
    }

    static empty(databaseName: string): replicationDestination {
        return new replicationDestination({
            Url: location.protocol + "//" + location.host,
            
            Database: databaseName,
            IgnoredClient: false,
            Disabled: false,
            ClientVisibleUrl: null,
            SpecifiedCollections: {} as { [key: string]: string; }
        } as Raven.Client.Documents.Replication.ReplicationDestination);
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

    toDto(): Raven.Client.Documents.Replication.ReplicationDestination {
        return {
            Url: this.prepareUrl(),
            Database: this.database(),
            IgnoredClient: this.ignoredClient(),
            Disabled: this.disabled(),
            ClientVisibleUrl: this.clientVisibleUrl(),
            SpecifiedCollections: this.enableReplicateOnlyFromCollections() ? this.specifiedCollectionsToMap() : null
        } as Raven.Client.Documents.Replication.ReplicationDestination;
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
    }*/
}

export = replicationDestination;
