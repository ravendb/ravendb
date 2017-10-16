/// <reference path="../../../../typings/tsd.d.ts"/>
import moment = require("moment");
import genUtils = require("common/generalUtils");

class patchDocument {

    name = ko.observable<string>("");
    query = ko.observable<string>("");
    selectedItem = ko.observable<string>("");
    patchAll = ko.observable<boolean>(true);

    toStorageDto(): storedPatchDto {

        const name = this.name();
        const query = this.query();
        const selectedItem = this.selectedItem();
        const patchAll = this.patchAll();

        return {
            Name: name,
            Query: query,
            SelectedItem: selectedItem,
            ModificationDate: moment().format("YYYY-MM-DD HH:mm"),
            PatchAll: patchAll,
            Hash: genUtils.hashCode(
                (name || "") +
                query +
                selectedItem +
                patchAll
            )
        } as storedPatchDto;
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
