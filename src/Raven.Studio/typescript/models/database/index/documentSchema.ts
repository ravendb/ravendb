/// <reference path="../../../../typings/tsd.d.ts"/>

class documentSchema {
    collectionName = ko.observable<string>();
    schema = ko.observable<string>();

    static create(collectionName: string, schema: string) {
        const item = new documentSchema();
        item.collectionName(collectionName);
        item.schema(schema);
        return item;
    }
}

export = documentSchema;

