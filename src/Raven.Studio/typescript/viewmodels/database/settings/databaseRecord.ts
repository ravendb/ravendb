import appUrl = require("common/appUrl");
import aceEditorBindingHandler = require("common/bindingHelpers/aceEditorBindingHandler");
import getDatabaseSettingsCommand = require("commands/resources/getDatabaseSettingsCommand");
import document = require("models/database/documents/document");
import database = require("models/resources/database");
import viewModelBase = require("viewmodels/viewModelBase");
import messagePublisher = require("common/messagePublisher");
import accessManager = require("common/shell/accessManager");
import eventsCollector = require("common/eventsCollector");

class databaseRecord extends viewModelBase {
    autoCollapseMode = ko.observable<boolean>(false);
    document = ko.observable<document>();
    documentText = ko.observable<string>().extend({ required: true });
    docEditor: AceAjax.Editor;
    securedSettings: string;
    updatedDto: documentDto;
    isForbidden = ko.observable<boolean>(false);

    static containerId ="#databaseSettingsContainer";

    constructor() {
        super();
        aceEditorBindingHandler.install();

        this.document.subscribe(doc => {
            if (doc) {
                const docDto: any = doc.toDto();
                this.securedSettings = ko.toJSON(docDto.SecuredSettings);
                const docText = this.stringify(doc.toDto());
                this.documentText(docText);
            }
        });
        
        this.bindToCurrentInstance("toggleAutoCollapse");
    }

    canActivate(args: any) {
        return $.when<any>(super.canActivate(args))
            .then(() => {
                const deferred = $.Deferred<canActivateResultDto>();

                this.isForbidden(!accessManager.default.clusterAdminOrClusterNode());

                if (this.isForbidden()) {
                    deferred.resolve({ can: true });
                } else {
                    const db: database = this.activeDatabase();
                    this.fetchDatabaseSettings(db)
                        .done(() => deferred.resolve({ can: true }))
                        .fail((response: JQueryXHR) => {
                            messagePublisher.reportError("Error fetching database settings!", response.responseText, response.statusText);
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

        var editorElement = $("#dbDocEditor");
        if (editorElement.length > 0) {
            this.docEditor = ko.utils.domData.get(editorElement[0], "aceEditor");
        }
    }

    toggleAutoCollapse() {
        this.autoCollapseMode.toggle();
        if (this.autoCollapseMode()) {
            this.foldAll();
        } else {
            this.docEditor.getSession().unfold(null, true);
        }
    }

    foldAll() {
        const AceRange = ace.require("ace/range").Range;
        this.docEditor.getSession().foldAll();
        const folds = <any[]> this.docEditor.getSession().getFoldsInRange(new AceRange(0, 0, this.docEditor.getSession().getLength(), 0));
        folds.map(f => this.docEditor.getSession().expandFold(f));
    }
    
    refreshFromServer() {
        eventsCollector.default.reportEvent("database-settings", "refresh");

        this.fetchDatabaseSettings(this.activeDatabase(), true);
    }

    private fetchDatabaseSettings(db: database, reportFetchProgress: boolean = false): JQueryPromise<any> {
        return new getDatabaseSettingsCommand(db, reportFetchProgress)
            .execute()
            .done((document: document) => this.document(document));
    }

    private stringify(obj: any) {
        const prettifySpacing = 4;
        return JSON.stringify(obj, (key, val) => {
            // strip out null properties
            return _.isNull(val) || _.isEqual(val, {}) ? undefined : val;
        }, prettifySpacing);
    }
}

export = databaseRecord;
