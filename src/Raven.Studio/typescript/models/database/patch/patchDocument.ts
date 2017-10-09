/// <reference path="../../../../typings/tsd.d.ts"/>

class patchDocument {

    name = ko.observable<string>("");
    query = ko.observable<string>("");
    selectedItem = ko.observable<string>("");
    patchAll = ko.observable<boolean>(true);

    toDto(): patchDto {
        return {
            Name: this.name(),
            Query: this.query(),
            SelectedItem: this.selectedItem(),
            ModificationDate: this.getCurrentTime(),
            PatchAll: this.patchAll()
        } as patchDto;
    }

    private getCurrentTime(): string {
        return new Date().toLocaleString().replace(/\//g, "-").replace(',', '');
    }

    copyFrom(incoming: patchDto) {
        this.name("");
        this.selectedItem(incoming.SelectedItem);
        this.query(incoming.Query);
        this.patchAll(incoming.PatchAll);
    }
}

export = patchDocument;
