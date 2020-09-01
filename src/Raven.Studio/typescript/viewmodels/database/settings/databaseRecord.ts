import appUrl = require("common/appUrl");
import aceEditorBindingHandler = require("common/bindingHelpers/aceEditorBindingHandler");
import getDatabaseRecordCommand = require("commands/resources/getDatabaseRecordCommand");
import document = require("models/database/documents/document");
import database = require("models/resources/database");
import viewModelBase = require("viewmodels/viewModelBase");
import messagePublisher = require("common/messagePublisher");
import accessManager = require("common/shell/accessManager");
import eventsCollector = require("common/eventsCollector");
import saveDatabaseRecordCommand = require("commands/resources/saveDatabaseRecordCommand");

class databaseRecord extends viewModelBase {
    isDocumentCollapsed = ko.observable<boolean>(false);
    forceFold: boolean = true;
    
    document = ko.observable<document>();
    documentText = ko.observable<string>().extend({ required: true });
    docEditor: AceAjax.Editor;
    isForbidden = ko.observable<boolean>(false);
    
    inEditMode = ko.observable<boolean>(false);

    static containerId = "#databaseRecordContainer";

    constructor() {
        super();
        aceEditorBindingHandler.install();

        this.document.subscribe(doc => {
            if (doc) {
                const docText = this.stringify(doc.toDto());
                this.documentText(docText);
            }
        });
        
        this.bindToCurrentInstance("toggleCollapse", "save", "exitEditMode");
    }

    canActivate(args: any) {
        return $.when<any>(super.canActivate(args))
            .then(() => {
                const deferred = $.Deferred<canActivateResultDto>();

                this.isForbidden(!accessManager.default.operatorAndAbove());

                if (this.isForbidden()) {
                    deferred.resolve({ can: true });
                } else {
                    const db: database = this.activeDatabase();
                    this.fetchDatabaseRecord(db)
                        .done(() => deferred.resolve({ can: true }))
                        .fail((response: JQueryXHR) => {
                            messagePublisher.reportError("Error fetching database record!", response.responseText, response.statusText);
                            deferred.resolve({ redirect: appUrl.forStatus(db) });
                        });
                }

                return deferred;
            });
    }

    activate(args: any) {
        super.activate(args);
        this.updateHelpLink('3QMLGH');
    }

    compositionComplete() {
        super.compositionComplete();

        this.docEditor = aceEditorBindingHandler.getEditorBySelection($("#dbDocEditor"));
        
        if (this.docEditor) {
            this.docEditor.getSession().on("tokenizerUpdate", () => {
                if (this.forceFold) {
                    this.foldAll();

                    this.forceFold = false;
                    this.isDocumentCollapsed(true);
                }
            });
       }
    }

    toggleCollapse() {
        if (this.isDocumentCollapsed()) {
            this.docEditor.getSession().unfold(null, true);
            this.isDocumentCollapsed(false);
        } else {
            this.foldAll();
            this.isDocumentCollapsed(true);
        }
    }

    foldAll() {
        const AceRange = ace.require("ace/range").Range;
        this.docEditor.getSession().foldAll();
        const folds = <any[]> this.docEditor.getSession().getFoldsInRange(new AceRange(0, 0, this.docEditor.getSession().getLength(), 0));
        folds.map(f => this.docEditor.getSession().expandFold(f));
    }
    
    refreshFromServer(reportFetchProgress: boolean = true) {
        eventsCollector.default.reportEvent("database-record", "refresh");
        this.fetchDatabaseRecord(this.activeDatabase(), reportFetchProgress);
        this.forceFold = true;
    }

    enterEditMode() {
        this.confirm()
            .done(result => {
                if (result.can) {
                    const docText = this.stringify(this.document().toDto(), false);
                    this.documentText(docText);
                    this.inEditMode(true);
                    this.isDocumentCollapsed(false);
                }
            })
    }
    
    exitEditMode() {
        const docText = this.stringify(this.document().toDto(), true);
        this.documentText(docText);
        this.inEditMode(false);
        this.isDocumentCollapsed(false);
    }
    
    confirm() {
        return this.confirmationMessage("Are you sure?", 
            "Tampering with the Database Record may result in unwanted behavior including loss of database along with all its data.",
            {
                buttons: ["Cancel", "Ok, I understand the risk"]
            });
    }
    
    save() {
        this.confirm()
            .then(result => {
                if (result.can) {
                    const dto = JSON.parse(this.documentText());
                    new saveDatabaseRecordCommand(this.activeDatabase(), dto, dto.Etag)
                        .execute()
                        .done(() => {
                            this.refreshFromServer(false);
                            this.exitEditMode();
                        });
                }
            });
    }

    private fetchDatabaseRecord(db: database, reportFetchProgress: boolean = false): JQueryPromise<any> {
        return new getDatabaseRecordCommand(db, reportFetchProgress)
            .execute()
            .done((document: document) => this.document(document));
    }

    private stringify(obj: any, stripNullAndEmptyValues: boolean = true) {
        const prettifySpacing = 4;
        
        if (stripNullAndEmptyValues) {
            return JSON.stringify(obj, function(key, val) {
                // Strip out null values for all entries except for 'Settings'
                if (this === obj.Settings) {
                    return _.isEqual(val, {}) ? undefined : val;
                }
                return _.isNull(val) || _.isEqual(val, {}) ? undefined : val;
            }, prettifySpacing);
        } else {
            return JSON.stringify(obj, null, prettifySpacing);
        }
    }
}

export = databaseRecord;
