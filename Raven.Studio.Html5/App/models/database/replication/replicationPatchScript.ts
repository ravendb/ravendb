class replicationPatchScript {

    constructor(collection: string, script: string) {
        this.collection(collection);
        this.script(script);
    }

    collection = ko.observable<string>();
    script = ko.observable<string>();

    collectionWithLabel = ko.computed(() => this.collection() || "Select collection");
}

export = replicationPatchScript;