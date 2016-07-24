/// <reference path="../../../../typings/tsd.d.ts"/>

import patchParam = require("models/database/patch/patchParam");
import document = require("models/database/documents/document");
import documentMetadata = require("models/database/documents/documentMetadata");

class patchDocument extends document {

    patchOnOption = ko.observable<string>();
    selectedItem = ko.observable<string>();
    query = ko.observable<string>();
    script = ko.observable<string>();
    parameters = ko.observableArray<patchParam>();
    allPatchOptions = ["Document", "Collection", "Index"];
    patchOptions: KnockoutComputed<string[]>;

    constructor(dto: patchDto) {
        super(dto);
        this.patchOnOption(dto.PatchOnOption);
        this.query(dto.Query);
        this.script(dto.Script);
        this.selectedItem(dto.SelectedItem);
        this.parameters(dto.Values.map(val => new patchParam(val)));

        this.patchOptions = ko.computed(() => this.allPatchOptions.filter(x => x !== this.patchOnOption()));
    }

    static empty() {
        var meta = {};
        meta['Raven-Entity-Name'] = 'PatchDocuments';
        return new patchDocument({
            '@metadata': meta,
            PatchOnOption: "Document",
            Query: "",
            Script: "",
            SelectedItem: "",
            Values: []
        });
    }

    toDto(): patchDto {
        var meta = this.__metadata.toDto();
        return {
            '@metadata': meta,
            PatchOnOption: this.patchOnOption(),
            Query: this.query(),
            Script: this.script(),
            SelectedItem: this.selectedItem(),
            Values: this.parameters().map(val => val.toDto())
        };
    }

    isDocumentPatch(): boolean {
        return this.patchOnOption() === "Document";
    }

    isCollectionPatch(): boolean {
        return this.patchOnOption() === "Collection";
    }

    isIndexPatch(): boolean {
        return this.patchOnOption() === "Index";
    }

    createParameter() {
        this.parameters.push(patchParam.empty());
    }

    removeParameter(key: patchParam) {
        this.parameters.remove(key);
    }

    name(): string {
        return this.__metadata.id.replace('Studio/Patch/', '');
    }

    resetMetadata() {
        this.__metadata = new documentMetadata();
        this.__metadata.ravenEntityName = 'PatchDocuments';
    }

    clone() {
        return new patchDocument(this.toDto());
    }
}

export = patchDocument;
