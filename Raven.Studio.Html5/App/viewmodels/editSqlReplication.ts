import router = require("plugins/router");
import viewModelBase = require("viewmodels/viewModelBase");
import appUrl = require("common/appUrl");
import jsonUtil = require("common/jsonUtil");
import aceEditorBindingHandler = require("common/aceEditorBindingHandler");
import messagePublisher = require("common/messagePublisher");
import app = require("durandal/app");
import database = require("models/database");
import collection = require("models/collection");
import sqlReplication = require("models/sqlReplication");
import getCollectionsCommand = require("commands/getCollectionsCommand");
import sqlReplicationStatsDialog = require("viewmodels/sqlReplicationStatsDialog");
import document = require("models/document");
import saveDocumentCommand = require("commands/saveDocumentCommand");
import deleteDocuments = require("viewmodels/deleteDocuments");
import getDocumentWithMetadataCommand = require("commands/getDocumentWithMetadataCommand");
import getDocumentsMetadataByIDPrefixCommand = require("commands/getDocumentsMetadataByIDPrefixCommand");
import documentMetadata = require("models/documentMetadata");
import resetSqlReplicationCommand = require("commands/resetSqlReplicationCommand");
import sqlReplicationSimulationDialog = require("viewmodels/sqlReplicationSimulationDialog");
import sqlReplicationConnections = require("models/sqlReplicationConnections");
import predefinedSqlConnection = require("models/predefinedSqlConnection");
import getEffectiveSqlReplicationConnectionStringsCommand = require("commands/getEffectiveSqlReplicationConnectionStringsCommand");


class editSqlReplication extends viewModelBase {
    
    static editSqlReplicationSelector = "#editSQLReplication";
    static sqlReplicationDocumentPrefix = "Raven/SqlReplication/Configuration/";

    editedReplication = ko.observable<sqlReplication>();
    collections = ko.observableArray<string>();
    areAllSqlReplicationsValid: KnockoutComputed<boolean>;
    isSaveEnabled: KnockoutComputed<boolean>;
    loadedSqlReplications: string[] = [];
    sqlReplicationName: KnockoutComputed<string>;
    isEditingNewReplication = ko.observable(false);
    isBasicView = ko.observable(true);
    availableConnectionStrings = ko.observableArray<predefinedSqlConnection>();
    sqlReplicationStatsAndMetricsHref = appUrl.forCurrentDatabase().statusDebugSqlReplication;
    appUrls: computedAppUrls;
    docEditor: AceAjax.Editor;
    script = ko.computed({
        read: () => {
            var r = this.editedReplication();
            return r ? r.script() : "";
        },
        write: v => this.editedReplication().script(v)
        });

    simulationDocumentId = ko.observable<string>();

    isBusy = ko.observable(false);
    initialReplicationId: string = '';

    constructor() {
        super();

        aceEditorBindingHandler.install();
        this.appUrls = appUrl.forCurrentDatabase();
        this.sqlReplicationName = ko.computed(() => (!!this.editedReplication() && !this.isEditingNewReplication()) ? this.editedReplication().name() : null);
    }

    private addScriptLabelPopover() {
        var popOverSettings: PopoverOptions = {
            html: true,
            trigger: 'hover',
			content: 'Replication scripts use JScript.<br/><br/>The script will be called once for each document in the source document collection, with <span class="code-keyword">this</span> representing the document, and the document id available as <i>documentId</i>.<br/><br/>Call <i>replicateToTableName</i> for each row you want to write to the database.<br/><br/>Example:</br><pre><span class="code-keyword">var</span> orderData = {<br/>   Id: documentId,<br/>   OrderLinesCount: <span class="code-keyword">this</span>.Lines.length,<br/>   TotalCost: 0<br/>};<br/><br/>for (<span class="code-keyword">var</span> i = 0; i &lt; <span class="code-keyword">this</span>.Lines.length; i++) {<br/>   <span class="code-keyword">var</span> line = <span class="code-keyword">this</span>.Lines[i];<br/>   <span class="code-keyword">var</span> lineCost = ((line.Quantity * line.PricePerUnit) * (1 - line.Discount));<br/>   orderData.TotalCost += lineCost;<br/><br/>   replicateToOrderLines({"<br/>      OrderId: documentId,<br/>      Qty: line.Quantity,<br/>      Product: line.Product,<br/>      Cost: lineCost<br/>   });<br/>}<br/><br/>replicateToOrders(orderData);</pre>',
            selector: '.script-label',
            placement: "right"
        };
        $('body').popover(popOverSettings);
        $('form :input[name="ravenEntityName"]').on("keypress", (e) => e.which != 13);
    }

    loadSqlReplicationConnections(): JQueryPromise<any> {
        return new getEffectiveSqlReplicationConnectionStringsCommand(this.activeDatabase())
            .execute()
            .done((dto: configurationDocumentDto<sqlReplicationConnectionsDto>) => {
                var connections = new sqlReplicationConnections(dto);

                if (connections.predefinedConnections().length > 0) {
                    connections.predefinedConnections().forEach((x: predefinedSqlConnection) => this.availableConnectionStrings.push(x));
                }
            });
    }

    canActivate(replicationToEditName: string) {
        var canActivateResult = $.Deferred();
        this.loadSqlReplicationConnections().always(() => {
            if (replicationToEditName) {
                this.loadSqlReplication(replicationToEditName)
                    .done(() => canActivateResult.resolve({ can: true }))
                    .fail(() => {
                        messagePublisher.reportError("Could not find " + decodeURIComponent(replicationToEditName) + " replication");
                        canActivateResult.resolve({ redirect: appUrl.forSqlReplications(this.activeDatabase()) });
                    });
            } else {
                this.isEditingNewReplication(true);
                this.editedReplication(this.createSqlReplication());
                this.fetchCollections(this.activeDatabase()).always(() => canActivateResult.resolve({ can: true }));
            }
        });
        return canActivateResult;
    }

    activate(replicationToEditName: string) {
        super.activate(replicationToEditName);
        this.dirtyFlag = new ko.DirtyFlag([this.editedReplication], false, jsonUtil.newLineNormalizingHashFunctionWithIgnoredFields(["__metadata", "metadata"]));
        this.isSaveEnabled = ko.computed(() => this.dirtyFlag().isDirty());
    }

    providerChanged(obj, event) {
        if (event.originalEvent && obj.connectionStringType() == obj.CONNECTION_STRING) {
            var curConnectionString = !!this.editedReplication().connectionStringValue() ? this.editedReplication().connectionStringValue().trim() : "";
            if (curConnectionString === "" ||
                sqlReplicationConnections.sqlProvidersConnectionStrings.first(x => x.ConnectionString == curConnectionString)) {
                var matchingConnectionStringPair: { ProviderName: string; ConnectionString: string; } = sqlReplicationConnections.sqlProvidersConnectionStrings.first(x => x.ProviderName == event.originalEvent.srcElement.selectedOptions[0].value);
                if (!!matchingConnectionStringPair) {
                    var matchingConnectionStringValue: string = matchingConnectionStringPair.ConnectionString;
                    this.editedReplication().connectionStringValue(
                        matchingConnectionStringValue
                        );
                }

            }
        }
    }


    loadSqlReplication(replicationToLoadName: string) {
        var loadDeferred = $.Deferred();
        $.when(this.fetchSqlReplicationToEdit(replicationToLoadName), this.fetchCollections(this.activeDatabase()))
            .done(() => {
                this.editedReplication().collections = this.collections;
                new getDocumentsMetadataByIDPrefixCommand(editSqlReplication.sqlReplicationDocumentPrefix, 256, this.activeDatabase())
                    .execute()
                    .done((results: string[]) => {
                        this.loadedSqlReplications = results;
                        loadDeferred.resolve();
                    }).
                    fail(() => loadDeferred.reject());
            })
            .fail(() => {
                loadDeferred.reject();
            });

        return loadDeferred;
    }

    fetchSqlReplicationToEdit(sqlReplicationName: string): JQueryPromise<any> {
        var loadDocTask = new getDocumentWithMetadataCommand(editSqlReplication.sqlReplicationDocumentPrefix + sqlReplicationName, this.activeDatabase()).execute();
        loadDocTask.done((document: document) => {
            var sqlReplicationDto: any = document.toDto(true);
            this.editedReplication(new sqlReplication(sqlReplicationDto));
            this.initialReplicationId = this.editedReplication().name();
            this.dirtyFlag().reset(); //Resync Changes
        });
        loadDocTask.always(() => this.isBusy(false));
        this.isBusy(true);
        return loadDocTask;
    }

    private fetchCollections(db: database): JQueryPromise<any> {
        return new getCollectionsCommand(db)
            .execute()
            .done((collections: Array<collection>) => {
                this.collections(collections.map((collection: collection) => { return collection.name; }));
            });
    }

    showStats() {
        var viewModel = new sqlReplicationStatsDialog(this.activeDatabase(), this.editedReplication().name());
        app.showDialog(viewModel);
    }

    refreshSqlReplication() {
        if (this.isEditingNewReplication() === false) {
            var docId = this.initialReplicationId;
            this.loadSqlReplication(docId);
        } else {

            this.editedReplication(this.createSqlReplication());
        }
    }

    compositionComplete() {
        super.compositionComplete();
        this.addScriptLabelPopover();
        $('pre').each((index, currentPreElement) => {
            this.initializeAceValidity(currentPreElement);
        });

        var editorElement = $("#sqlReplicationEditor");
        if (editorElement.length > 0) {
            this.docEditor = ko.utils.domData.get(editorElement[0], "aceEditor");
        }

        $("#sqlReplicationEditor").on('DynamicHeightSet', () => this.docEditor.resize());

    }

    createSqlReplication(): sqlReplication {
        var newSqlReplication: sqlReplication = sqlReplication.empty();
        newSqlReplication.collections(this.collections());
        this.collections.subscribe(value => newSqlReplication.collections(value));
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

    detached() {
        super.detached();
        $("#sqlReplicationEditor").off('DynamicHeightSet');
    }


    private isSqlReplicationNameExists(name): boolean {
        var count = 0;
        return !!this.loadedSqlReplications.first(x=> x == name);
    }

    private initializeAceValidity(element: Element) {
        var editorElement = $("#aceEditor");
        if (editorElement.length > 0) {
            var editor = ko.utils.domData.get(editorElement[0], "aceEditor");
            var editorValue = editor.getSession().getValue();
            if (editorValue === "") {
                var textarea: any = $(element).find('textarea')[0];
                textarea.setCustomValidity("Please fill out this field.");
            }
        }
    }

    save() {
        var currentDocumentId = this.editedReplication().name();
        this.editedReplication().script(this.script());

        if (this.initialReplicationId !== currentDocumentId) {
            delete this.editedReplication().__metadata.etag;
            delete this.editedReplication().__metadata.lastModified;
        }

        var newDoc = new document(this.editedReplication().toDto());
        newDoc.__metadata = new documentMetadata();
        this.attachReservedMetaProperties(editSqlReplication.sqlReplicationDocumentPrefix + currentDocumentId, newDoc.__metadata);

        var saveCommand = new saveDocumentCommand(editSqlReplication.sqlReplicationDocumentPrefix + currentDocumentId, newDoc, this.activeDatabase());
        var saveTask = saveCommand.execute();
        saveTask.done((saveResult: bulkDocumentDto[]) => {
            var savedDocumentDto: bulkDocumentDto = saveResult[0];
            var sqlReplicationKey = savedDocumentDto.Key.substring(editSqlReplication.sqlReplicationDocumentPrefix.length);
            this.loadSqlReplication(sqlReplicationKey)
                .done(() => this.dirtyFlag().reset());
            this.updateUrl(sqlReplicationKey);

            this.isEditingNewReplication(false);
            this.initialReplicationId = currentDocumentId;
        });
    }


    updateUrl(docId: string) {
        var url = appUrl.forEditSqlReplication(docId, this.activeDatabase());
        router.navigate(url, false);
    }

    attachReservedMetaProperties(id: string, target: documentMetadata) {
        target.etag = '';
        target.ravenEntityName = !target.ravenEntityName ? document.getEntityNameFromId(id) : target.ravenEntityName;
        target.id = id;
    }

    deleteSqlReplication() {
        var newDoc = new document(this.editedReplication().toDto());

        if (newDoc) {
            var viewModel = new deleteDocuments([newDoc]);
            viewModel.deletionTask.done(() => {
                this.dirtyFlag().reset(); //Resync Changes
                router.navigate(appUrl.forCurrentDatabase().sqlReplications());
            });
            app.showDialog(viewModel, editSqlReplication.editSqlReplicationSelector);

        }
    }
    resetSqlReplication() {

        app.showMessage("You are about to reset this SQL Replication, forcing replication of all collection items", "SQL Replication Reset", ["Cancel", "Reset"])
            .then((dialogResult: string) => {
                if (dialogResult === "Reset") {
                    var replicationId = this.initialReplicationId;
                    new resetSqlReplicationCommand(this.activeDatabase(), replicationId).execute()
                        .done(() => messagePublisher.reportSuccess("SQL replication " + replicationId + " was reset successfully!"))
                        .fail(() => messagePublisher.reportError("SQL replication " + replicationId + " failed to reset!"));
                }
            });

    }

    simulateSqlReplication() {
        this.editedReplication().script(this.script());
        var viewModel = new sqlReplicationSimulationDialog(this.activeDatabase(), this.editedReplication(), this.simulationDocumentId);
        app.showDialog(viewModel);
    }

    getSqlReplicationConnectionStringsUrl(sqlReplicationName: string) {
        return appUrl.forSqlReplicationConnections(this.activeDatabase());
    }

}

export = editSqlReplication; 