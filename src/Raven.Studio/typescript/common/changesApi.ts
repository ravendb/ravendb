/// <reference path="../../typings/tsd.d.ts" />

import database = require("models/resources/database");
import changeSubscription = require("common/changeSubscription");
import changesCallback = require("common/changesCallback");

import EVENTS = require("common/constants/events");

import eventsWebSocketClient = require("common/eventsWebSocketClient");

class changesApi extends eventsWebSocketClient<changesApiEventDto[]> {

    constructor(db: database) {
        super(db);
    }

    private allDocsHandlers = ko.observableArray<changesCallback<Raven.Client.Documents.Changes.DocumentChange>>();
    private allIndexesHandlers = ko.observableArray<changesCallback<Raven.Client.Documents.Changes.IndexChange>>();

    private watchedDocuments = new Map<string, KnockoutObservableArray<changesCallback<Raven.Client.Documents.Changes.DocumentChange>>>();
    private watchedPrefixes = new Map<string, KnockoutObservableArray<changesCallback<Raven.Client.Documents.Changes.DocumentChange>>>();
    private watchedIndexes = new Map<string, KnockoutObservableArray<changesCallback<Raven.Client.Documents.Changes.IndexChange>>>();

    get connectionDescription() {
        return this.db.fullTypeName + " = " + this.db.name;
    }

    protected webSocketUrlFactory() {
        const connectionString = "throttleConnection=true";
        return "/changes?" + connectionString;
    }

    protected onOpen() {
        super.onOpen();
        ko.postbox.publish(EVENTS.ChangesApi.Reconnected, this.db);
    }

    protected onMessage(eventsDto: changesApiEventDto[]) {
        eventsDto.forEach(event => this.onSingleMessage(event));
    }

    private onSingleMessage(eventDto: changesApiEventDto) {
        const eventType = eventDto.Type;
        const value = eventDto.Value;

        if (!eventType) {
            return;
        }
        
        switch (eventType) {
            case "DocumentChange":
                this.fireEvents<Raven.Client.Documents.Changes.DocumentChange>(this.allDocsHandlers(), value, () => true);

                this.watchedDocuments.forEach((callbacks, id) => {
                    this.fireEvents<Raven.Client.Documents.Changes.DocumentChange>(callbacks(), value, (event) => event.Id != null && event.Id === id);
                });

                this.watchedPrefixes.forEach((callbacks, id) => {
                    this.fireEvents<Raven.Client.Documents.Changes.DocumentChange>(callbacks(), value, (event) => event.Id != null && event.Id.startsWith(id));
                });
                break;
            case "IndexChange":
                this.fireEvents<Raven.Client.Documents.Changes.IndexChange>(this.allIndexesHandlers(), value, () => true);

                this.watchedIndexes.forEach((callbacks, key) => {
                    this.fireEvents<Raven.Client.Documents.Changes.IndexChange>(callbacks(), value, (event) => event.Name != null && event.Name === key);
                });
                break;
            default:
                console.log("Unhandled Changes API notification type: " + eventType);
        }
    }

    watchAllIndexes(onChange: (e: Raven.Client.Documents.Changes.IndexChange) => void) {
        var callback = new changesCallback<Raven.Client.Documents.Changes.IndexChange>(onChange);
        if (this.allIndexesHandlers().length === 0) {
            this.send("watch-indexes");
        }
        this.allIndexesHandlers.push(callback);
        return new changeSubscription(() => {
            this.allIndexesHandlers.remove(callback);
            if (this.allIndexesHandlers().length === 0) {
                this.send("unwatch-indexes");
            }
        });
    }

    watchIndex(indexName: string, onChange: (e: Raven.Client.Documents.Changes.IndexChange) => void): changeSubscription {
        let callback = new changesCallback<Raven.Client.Documents.Changes.IndexChange>(onChange);

        if (!this.watchedIndexes.has(indexName)) {
            this.send("watch-index", indexName);
            this.watchedIndexes.set(indexName, ko.observableArray<changesCallback<Raven.Client.Documents.Changes.IndexChange>>());
        }

        let callbacks = this.watchedIndexes.get(indexName);
        callbacks.push(callback);

        return new changeSubscription(() => {
            callbacks.remove(callback);
            if (callbacks().length === 0) {
                this.watchedIndexes.delete(indexName);
                this.send("unwatch-index", indexName);
            }
        });
    }

    watchAllDocs(onChange: (e: Raven.Client.Documents.Changes.DocumentChange) => void) {
        var callback = new changesCallback<Raven.Client.Documents.Changes.DocumentChange>(onChange);

        if (this.allDocsHandlers().length === 0) {
            this.send("watch-docs");
        }

        this.allDocsHandlers.push(callback);

        return new changeSubscription(() => {
            this.allDocsHandlers.remove(callback);
            if (this.allDocsHandlers().length === 0) {
                this.send("unwatch-docs");
            }
        });
    }

    watchDocument(docId: string, onChange: (e: Raven.Client.Documents.Changes.DocumentChange) => void): changeSubscription {
        let callback = new changesCallback<Raven.Client.Documents.Changes.DocumentChange>(onChange);

        if (!this.watchedDocuments.has(docId)) {
            this.send("watch-doc", docId);
            this.watchedDocuments.set(docId, ko.observableArray<changesCallback<Raven.Client.Documents.Changes.DocumentChange>>());
        }

        let callbacks = this.watchedDocuments.get(docId);
        callbacks.push(callback);

        return new changeSubscription(() => {
            callbacks.remove(callback);
            if (callbacks().length === 0) {
                this.watchedDocuments.delete(docId);
                this.send("unwatch-doc", docId);
            }
        });
    }

    watchDocsStartingWith(docIdPrefix: string, onChange: (e: Raven.Client.Documents.Changes.DocumentChange) => void): changeSubscription {
        let callback = new changesCallback<Raven.Client.Documents.Changes.DocumentChange>(onChange);

        if (!this.watchedPrefixes.has(docIdPrefix)) {
            this.send("watch-prefix", docIdPrefix);
            this.watchedPrefixes.set(docIdPrefix, ko.observableArray<changesCallback<Raven.Client.Documents.Changes.DocumentChange>>());
        }

        let callbacks = this.watchedPrefixes.get(docIdPrefix);
        callbacks.push(callback);

        return new changeSubscription(() => {
            callbacks.remove(callback);
            if (callbacks().length === 0) {
                this.watchedPrefixes.delete(docIdPrefix);
                this.send("unwatch-prefix", docIdPrefix);
            }
        });
    }
}

export = changesApi;

