/// <reference path="../../../../typings/tsd.d.ts"/>
import moment = require("moment");
import genUtils = require("common/generalUtils");

class patchDocument {

    name = ko.observable<string>("");
    query = ko.observable<string>("");
    selectedItem = ko.observable<string>("");
    patchAll = ko.observable<boolean>(true);

    toStorageDto(): storedPatchDto {

        const _name = this.name();
        const _query = this.query();
        const _selectedItem = this.selectedItem();
        const _patchAll = this.patchAll();

        return {
            Name: _name,
            Query: _query,
            SelectedItem: _selectedItem,
            ModificationDate: moment().format("YYYY-MM-DD HH:mm"),
            PatchAll: _patchAll,
            Hash: genUtils.hashCode(
                (_name || "") +
                _query +
                _selectedItem +
                _patchAll
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
