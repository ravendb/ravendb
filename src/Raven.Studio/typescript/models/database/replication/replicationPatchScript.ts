/// <reference path="../../../../typings/tsd.d.ts"/>

import collection = require("models/database/documents/collection");

class replicationPatchScript {

    constructor(collection: string, script: string) {
        this.collection(collection);
        this.script(script);
    }

    static empty() {
        return new replicationPatchScript(undefined, undefined);
    }

    collection = ko.observable<string>();
    script = ko.observable<string>();

    hasScript = ko.pureComputed(() => typeof (this.script()) !== "undefined");

    collectionWithLabel = ko.computed(() => this.collection() || "Select collection");

    toggleScript() {
        if (typeof (this.script()) === "undefined") {
            this.script("");
        } else {
            this.script(undefined);
        }
    }

    createSearchResults(collections: KnockoutObservableArray<collection>): KnockoutComputed<string[]> {
        return ko.computed(() => {
            if (this.collection()) {
                var collectionToLower = this.collection().toLowerCase();
                return collections()
                    .filter(collection => collection.name.toLowerCase().indexOf(collectionToLower) === 0)
                    .map(x => x.name);
            } else {
                return collections().map(x => x.name);
            }
           
        });
    }

}

export = replicationPatchScript;
