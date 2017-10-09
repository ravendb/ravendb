/// <reference path="../../../../typings/tsd.d.ts"/>

import genUtils = require("common/generalUtils");

class patchDocument {

    name = ko.observable<string>();
    selectedItem = ko.observable<string>();
    query = ko.observable<string>();
    patchAll = ko.observable<boolean>(true);

    constructor(dto: patchDto) {
        this.name(dto.Name);
        this.query(dto.Query);
        this.selectedItem(dto.SelectedItem);
        this.patchAll(dto.PatchAll);
    }

    static empty() {
        return new patchDocument({
            Name: "",
            Query: "",
            SelectedItem: "",
            ModificationDate: new Date(),
            PatchAll: true
    });
    }

    toDto(): patchDto {
        return {
            Name: this.name(),
            Query: this.query(),
            SelectedItem: this.selectedItem(),
            ModificationDate: new Date(),
            PatchAll: this.patchAll()
        } as patchDto;
    }

    copyFrom(incoming: patchDocument) {
        this.name(incoming.name());
        this.selectedItem(incoming.selectedItem());
        this.query(incoming.query());
        this.patchAll(incoming.patchAll());
    }
}

export = patchDocument;
