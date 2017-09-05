/// <reference path="../../../../typings/tsd.d.ts"/>

import document = require("models/database/documents/document");

class patchDocument extends document {

    selectedItem = ko.observable<string>();
    query = ko.observable<string>();

    patchAll = ko.observable<boolean>(true);

    constructor(dto: patchDto) {
        super(dto);
        this.query(dto.Query);
        this.selectedItem(dto.SelectedItem);
    }

    static empty() {
        const meta: any = {};
        meta['@collection'] = 'PatchDocuments';
        return new patchDocument({
            '@metadata': meta,
            Query: "",
            SelectedItem: "",
            Values: []
        });
    }

    toDto(): patchDto {
        const meta = this.__metadata.toDto();
        return {
            '@metadata': meta,
            Query: this.query(),
            SelectedItem: this.selectedItem()
        };
    }

    name(): string {
        return this.__metadata.id.replace('Raven/Studio/Patch/', '');
    }

    modificationDate(): string {
        return this.__metadata.lastModifiedFullDate();
    }

    copyFrom(incoming: patchDocument) {
        this.selectedItem(incoming.selectedItem());
        this.__metadata = incoming.__metadata;
        this.query(incoming.query());
        this.patchAll(true);
        this.__metadata.changeVector(undefined);
    }
}

export = patchDocument;
