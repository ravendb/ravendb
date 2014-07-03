/// <reference path="../models/dto.ts" />
import dialogViewModelBase = require("viewmodels/dialogViewModelBase");
import simulateSqlReplicationCommand = require("commands/simulateSqlReplicationCommand");
import database = require("models/database");
import document = require("models/document");
import getDocumentsMetadataByIDPrefixCommand = require("commands/getDocumentsMetadataByIDPrefixCommand");
import dialog = require("plugins/dialog");
import collection = require("models/collection");

class sqlReplicationSimulationDialog extends dialogViewModelBase {

    simulationResults = ko.observableArray<string>();
    documentAutocompletes = ko.observableArray<string>();
    documentId = ko.observable<string>();
    lastSearchedDocumentID = ko.observable<string>("");
    isAutoCompleteVisible : KnockoutComputed<boolean>;
    
    constructor(private db: database, private sqlReplicationName: string) {
        super();
        this.documentId.throttle(250).subscribe(search => this.fetchDocumentIdAutocompletes(search));
        this.isAutoCompleteVisible = ko.computed(() => {
            return this.lastSearchedDocumentID() !== this.documentId() && 
                (this.documentAutocompletes().length > 1 || this.documentAutocompletes().length == 1 && this.documentId() !== this.documentAutocompletes()[0]);
        });
    }

    getResults() {
        this.lastSearchedDocumentID(this.documentId());
        new simulateSqlReplicationCommand(this.db, this.sqlReplicationName, this.documentId())
            .execute()
            .done((results: string[]) => this.simulationResults(results))
            .fail(() => this.simulationResults.removeAll());
    }

    // overrid dialogViewModelBase shortcuts behavior
   attached() {
       $("#docIdInput").focus();
       var that = this;
       jwerty.key("esc", e => {
           e.preventDefault();
           dialog.close(that);
       }, this, this.dialogSelectorName == "" ? dialogViewModelBase.dialogSelector : this.dialogSelectorName);
   }

    fetchDocumentIdAutocompletes(query: string) {
        if (query.length >= 2) {
            new getDocumentsMetadataByIDPrefixCommand(query, 10, this.db)
                .execute()
                .done((results: string[]) => {
                    if (this.documentId() === query) {
                        if (results.length == 1 && this.documentId() == results[0]) {
                            this.documentAutocompletes.removeAll();
                            return;
                        }
                        this.documentAutocompletes(results);
                    }
                });
        } else if (query.length == 0) {
            this.documentAutocompletes.removeAll();
        }
    }

    documentIdSubmitted(submittedDocumentId) {
        this.documentId(submittedDocumentId);
        $('#docIdInput').focus();
        this.getResults();
    }

    getDocCssClass(doc: documentMetadataDto) {
        return collection.getCollectionCssClass(doc['@metadata']['Raven-Entity-Name']);
    }

    keyPressedOnDocumentAutocomplete(doc: documentMetadataDto, event) {
        if (event.keyCode == 13 && !!doc) {
            var docId = !!doc['@metadata'] ? doc['@metadata']['@id'] : null;
            if (!!docId) {
                this.documentId(docId);
            }
        }
    }

    cancel() {
        dialog.close(this);
    }


}

export = sqlReplicationSimulationDialog;