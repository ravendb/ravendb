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
    
    hideEmptyValues = ko.observable<boolean>(false);
    selectedHideState: boolean;

    static containerId = "#databaseRecordContainer";

    constructor() {
        super();
        aceEditorBindingHandler.install();

        this.document.subscribe(document => {
            if (document) {
                this.setVisibleDocumentText();
            }
        });
        
        this.hideEmptyValues.subscribe(() => {
            this.setVisibleDocumentText();
        })
        
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
                    this.forceFold = false;
                    this.collapseDocument();
                }
            });
       }
    }

    toggleCollapse() {
        if (this.isDocumentCollapsed()) {
            this.unfoldDocument();
        } else {
            this.collapseDocument();
        }
    }
    
    private collapseDocument() {
        if (this.docEditor) {
            this.foldAll();
            this.isDocumentCollapsed(true);
        }
    }
    
    private unfoldDocument() {
        if (this.docEditor) {
            this.docEditor.getSession().unfold(null, true);
            this.isDocumentCollapsed(false);
        }
    }

    private foldAll() {
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
                    this.inEditMode(true);
                    this.selectedHideState = this.hideEmptyValues();
                    this.hideEmptyValues(false);
                    this.unfoldDocument();
                }
            })
    }
    
    exitEditMode() {
        this.inEditMode(false);
        this.hideEmptyValues(this.selectedHideState);
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
    
    private setVisibleDocumentText() {
        const docText = this.stringify(this.document().toDto(), this.hideEmptyValues());
        this.documentText(docText);

        // must keep the collapse state because although user didn't actively changed it, 
        // the documentText has changed and it changes the ace editor state
        if (this.isDocumentCollapsed()) {
            this.collapseDocument()
        } else {
            this.unfoldDocument();
        }
    }

    private stringify(obj: any, stripNullAndEmptyValues: boolean = false) {
        const prettifySpacing = 4;
        
        if (stripNullAndEmptyValues) {
            return JSON.stringify(obj, (key, val) => {
                const isNull = _.isNull(val);
                const isEmptyObj = _.isEqual(val, {});
                const isEmptyArray = _.isEqual(val, []);
                
                return isNull || isEmptyObj || isEmptyArray ? undefined : val;
                
            }, prettifySpacing);
        } else {
            return JSON.stringify(obj, null, prettifySpacing);
        }
    }
}

export = databaseRecord;
