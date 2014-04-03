import patchParam = require("models/patchParam");

class patchDocuments {

    patchOnOption = ko.observable<string>();
    selectedItem = ko.observable<string>();
    query = ko.observable<string>();
    script = ko.observable<string>();
    beforePatch = ko.observable<string>();
    afterPatch = ko.observable<string>();
    parameters = ko.observableArray<patchParam>();

    constructor(dto: patchDto) {
        this.patchOnOption(dto.PatchOnOption);
        this.query(dto.Query);
        this.script(dto.Script);
        this.selectedItem(dto.SelectedItem);
        this.parameters(dto.Values.map(val => new patchParam(val)));
    }

    static empty() {
        return new patchDocuments({
            PatchOnOption: "Document",
            Query: "",
            Script: "",
            SelectedItem: "",
            Values: []
        });
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
}

export = patchDocuments;