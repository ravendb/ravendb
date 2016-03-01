/// <reference path="../../../../typings/tsd.d.ts"/>

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

    collectionWithLabel = ko.computed(() => this.collection() || "Select collection");
}

export = replicationPatchScript;
