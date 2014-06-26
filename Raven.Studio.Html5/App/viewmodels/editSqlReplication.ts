import router = require("plugins/router");
import viewModelBase = require("viewmodels/viewModelBase");
import appUrl = require("common/appUrl");
import dialog = require("plugins/dialog");
import aceEditorBindingHandler = require("common/aceEditorBindingHandler");
import alertType = require("common/alertType");
import alertArgs = require("common/alertArgs");
import app = require("durandal/app");

import database = require("models/database");
import collection = require("models/collection");
import sqlReplication = require("models/sqlReplication");
import getSqlReplicationsCommand = require("commands/getSqlReplicationsCommand");
import saveSqlReplicationsCommand = require("commands/saveSqlReplicationsCommand");
import deleteDocumentsCommand = require("commands/deleteDocumentsCommand");
import getCollectionsCommand = require("commands/getCollectionsCommand");
import ace = require("ace/ace");
import sqlReplicationStatsDialog = require("viewmodels/sqlReplicationStatsDialog");

import sys = require("durandal/system");


import document = require("models/document");
import saveDocumentCommand = require("commands/saveDocumentCommand");
import deleteDocuments = require("viewmodels/deleteDocuments");
import pagedList = require("common/pagedList");
import getDocumentWithMetadataCommand = require("commands/getDocumentWithMetadataCommand");
import verifyDocumentsIDsCommand = require("commands/verifyDocumentsIDsCommand");
import genUtils = require("common/generalUtils");
import queryIndexCommand = require("commands/queryIndexCommand");
import pagedResultSet = require("common/pagedResultSet");

import getDocumentsMetadataByIDPrefixCommand = require("commands/getDocumentsMetadataByIDPrefixCommand");
import documentMetadata = require("models/documentMetadata");


class editSqlReplication extends viewModelBase {

    editedReplication = ko.observable<sqlReplication>();
    collections = ko.observableArray<string>();
    areAllSqlReplicationsValid: KnockoutComputed<boolean>;
    isSaveEnabled: KnockoutComputed<boolean>;
    loadedSqlReplications: string[] = [];
    sqlReplicationName: KnockoutComputed<string>;
    isEditingNewReplication = ko.observable(false);
    
    appUrls: computedAppUrls;

    isBusy = ko.observable(false);
    initialReplicationId:string='';

    constructor() {
        super();

        aceEditorBindingHandler.install();
        this.appUrls = appUrl.forCurrentDatabase();
        this.sqlReplicationName = ko.computed(() => (!!this.editedReplication() && !this.isEditingNewReplication()) ? this.editedReplication().name() : null);
    }

    

    canActivate(replicationToEditName: string) {
        if (replicationToEditName) {
            var canActivateResult = $.Deferred();
            this.loadSqlReplication(replicationToEditName)
                .done(() => canActivateResult.resolve({ can: true }))
                .fail(() => {
                    ko.postbox.publish("Alert", new alertArgs(alertType.danger, "Could not find " + decodeURIComponent(replicationToEditName) + " replication", null));
                    canActivateResult.resolve({ redirect: appUrl.forSqlReplications(this.activeDatabase()) });
                });

            return canActivateResult;
        } else {
            return $.Deferred().resolve({ can: true });
        }
    }

    activate(replicationToEditName: string) {
        super.activate(replicationToEditName);

//        this.isEditingExistingReplication(replicationToEditName != null);

        if (!replicationToEditName) {
            this.editedReplication(this.createSqlReplication());
        }

        viewModelBase.dirtyFlag = new ko.DirtyFlag([this.editedReplication]);

        this.isSaveEnabled = ko.computed(() => {
            return viewModelBase.dirtyFlag().isDirty();
        });
        
    }
    
   
    loadSqlReplication(replicationToLoadName: string) {
        var loadDeferred = $.Deferred();

        this.fetchSqlReplicationToEdit(replicationToLoadName)
            .done(() => {
                new getDocumentsMetadataByIDPrefixCommand("Raven/SqlReplication/Configuration/", 256, this.activeDatabase())
                    .execute()
                    .done((results: string[]) => {
                        this.loadedSqlReplications = results;
                        loadDeferred.resolve();
                    }).
                    fail(() => loadDeferred.reject());
            })
            .fail(() => {
            debugger;
            loadDeferred.reject();
        });

        return loadDeferred;
    }

    fetchSqlReplicationToEdit(sqlReplicationName: string): JQueryPromise<any> {
        var loadDocTask = new getDocumentWithMetadataCommand("Raven/SqlReplication/Configuration/" + sqlReplicationName, this.activeDatabase()).execute();
        loadDocTask.done((document: document) => {
            var sqlReplicationDto: any = document.toDto(true);
            this.editedReplication(new sqlReplication(sqlReplicationDto));
            this.initialReplicationId = this.editedReplication().getId();
            viewModelBase.dirtyFlag().reset(); //Resync Changes
        });
        loadDocTask.always(() => this.isBusy(false));
        this.isBusy(true);
        return loadDocTask;
    }

    showStats() {
        alert("showStats");
    }

    refreshSqlReplication() {
        alert("refreshSqlReplication");
    }

    compositionComplete() {
        super.compositionComplete();

        $('pre').each((index, currentPreElement) => {
            this.initializeAceValidity(currentPreElement);
        });
    }
    
    createSqlReplication(): sqlReplication {
        var newSqlReplication: sqlReplication = sqlReplication.empty();
        newSqlReplication.collections = this.collections;
        this.subscribeToSqlReplicationName(newSqlReplication);
        return newSqlReplication;
    }


    private subscribeToSqlReplicationName(sqlReplicationElement: sqlReplication) {
        sqlReplicationElement.name.subscribe((previousName) => {
            //Get the previous value of 'name' here before it's set to newValue
            var nameInputArray = $('input[name="name"]').filter(function () { return this.value === previousName; });
            if (nameInputArray.length === 1) {
                var inputField: any = nameInputArray[0];
                inputField.setCustomValidity("");
            }
        }, this, "beforeChange");
        sqlReplicationElement.name.subscribe((newName) => {
            var message = "";
            if (newName === "") {
                message = "Please fill out this field.";
            }
            else if (this.isSqlReplicationNameExists(newName)) {
                message = "SQL Replication name already exists.";
            }
            $('input[name="name"]')
                .filter(function () { return this.value === newName; })
                .each((index, element: any) => {
                    element.setCustomValidity(message);
                });
        });
    }

    private isSqlReplicationNameExists(name): boolean {
        var count = 0;
        return !!this.loadedSqlReplications.first(x=>x==name);
    }

    private initializeAceValidity(element: Element) {
        var editor: AceAjax.Editor = ko.utils.domData.get(element, "aceEditor");
        if (editor)
        {
        var editorValue = editor.getSession().getValue();
        if (editorValue === "") {
            var textarea: any = $(element).find('textarea')[0];
            textarea.setCustomValidity("Please fill out this field.");
        }
        }
    }

    save() {
        var currentDocumentId = this.editedReplication().name();

        if (this.initialReplicationId !== currentDocumentId) {
            delete this.editedReplication().__metadata.etag;
            delete this.editedReplication().__metadata.lastModified;
        }
        
        var newDoc = new document(this.editedReplication().toDto());

        
        var saveCommand = new saveDocumentCommand("Raven/SqlReplication/Configuration/" + currentDocumentId, newDoc, this.activeDatabase());
        var saveTask = saveCommand.execute();
        saveTask.done((idAndEtag: { Key: string; ETag: string }) => {
            viewModelBase.dirtyFlag().reset(); //Resync Changes
            this.loadSqlReplication(idAndEtag.Key);
            this.updateUrl(idAndEtag.Key);
            this.isEditingNewReplication(false);
            //deleteDocumentCommand - todo: delete previous document if was renamed
        });
    }

    updateUrl(indexName: string) {
//        if (indexName != null)
//            router.navigate(appUrl.forEditIndex(indexName, this.activeDatabase()));
    }

    refreshDocument() {
//        if (this.isInDocMode()) {
//            if (!this.isCreatingNewDocument()) {
//                var docId = this.editedDocId();
//                this.document(null);
//                this.documentText(null);
//                this.metadataText(null);
//                this.userSpecifiedId('');
//                this.loadDocument(docId);
//            } else {
//                this.editNewDocument();
//            }
//        } else {
//            this.queryResultList().getNthItem(this.currentQueriedItemIndex).done((doc) => this.document(doc));
//            this.lodaedDocumentName("");
//        }
    }

    deleteSqlReplication() {
        alert("delete");
    }
    deleteDocument() {
//        var doc: document = this.document();
//        if (doc) {
//            var viewModel = new deleteDocuments([doc]);
//            viewModel.deletionTask.done(() => {
//                viewModelBase.dirtyFlag().reset(); //Resync Changes
//
//                var list = this.docsList();
//                if (!!list) {
//                    this.docsList().invalidateCache();
//
//                    var newTotalResultCount = list.totalResultCount() - 1;
//                    list.totalResultCount(newTotalResultCount);
//
//                    var nextIndex = list.currentItemIndex();
//                    if (nextIndex >= newTotalResultCount) {
//                        nextIndex = 0;
//                    }
//
//                    this.pageToItem(nextIndex, newTotalResultCount);
//                }
//            });
//            app.showDialog(viewModel, editDocument.editDocSelector);
//        }
    }
    

    copyIndex() {
//        app.showDialog(new copyIndexDialog(this.editedIndex().name(), this.activeDatabase(), false));
    }



}

export = editSqlReplication; 