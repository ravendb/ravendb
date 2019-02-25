import aceEditorBindingHandler = require("common/bindingHelpers/aceEditorBindingHandler");
import viewModelBase = require("viewmodels/viewModelBase");
import endpoints = require("endpoints");
import database = require("models/resources/database");
import messagePublisher = require("common/messagePublisher");
import importDatabaseCommand = require("commands/database/studio/importDatabaseCommand");
import importDatabaseModel = require("models/database/tasks/importDatabaseModel");
import notificationCenter = require("common/notifications/notificationCenter");
import eventsCollector = require("common/eventsCollector");
import appUrl = require("common/appUrl");
import copyToClipboard = require("common/copyToClipboard");
import getNextOperationId = require("commands/database/studio/getNextOperationId");
import EVENTS = require("common/constants/events");
import popoverUtils = require("common/popoverUtils");
import defaultAceCompleter = require("common/defaultAceCompleter");
import getDatabaseCommand = require("commands/resources/getDatabaseCommand");
import validateSmugglerOptionsCommand = require("commands/database/studio/validateSmugglerOptionsCommand");
import collectionsTracker = require("common/helpers/database/collectionsTracker");
import viewHelpers = require("common/helpers/view/viewHelpers");

class importDatabaseFromFile extends viewModelBase {

    private static readonly filePickerTag = "#importDatabaseFilePicker";

    model = new importDatabaseModel();
    completer = defaultAceCompleter.completer();

    static isImporting = ko.observable(false);
    isImporting = importDatabaseFromFile.isImporting;

    showAdvancedOptions = ko.observable(false);
    showTransformScript = ko.observable(false);

    hasFileSelected = ko.observable(false);
    importedFileName = ko.observable<string>();

    isUploading = ko.observable<boolean>(false);
    uploadStatus = ko.observable<number>();

    importCommandPowerShell: KnockoutComputed<string>;
    importCommandCmd: KnockoutComputed<string>;
    importCommandBash: KnockoutComputed<string>;

    validationGroup = ko.validatedObservable({
        importedFileName: this.importedFileName,
        transformScript: this.model.transformScript
    });

    constructor() {
        super();

        this.bindToCurrentInstance("copyCommandToClipboard", "fileSelected", "customizeConfigurationClicked");

        aceEditorBindingHandler.install();
        this.isUploading.subscribe(v => {
            if (!v) {
                this.uploadStatus(0);
            }
        });

        this.showTransformScript.subscribe(v => {
            if (v) {
                this.model.transformScript(
                    "this.collection = this['@metadata']['@collection'];\r\n" +
                    "// current object is available under 'this' variable\r\n" +
                    "// @change-vector, @id, @last-modified metadata fields are not available");
            } else {
                this.model.transformScript("");
            }
        });
        
        const modelAsJson = () => {
            const args = this.model.toDto();
            if (!args.TransformScript) {
                delete args.TransformScript;
            }
            return JSON.stringify(args);
        };
        
        const fileNameProvider = () => this.importedFileName() || "Dump of Database.ravendbdump";
        const commandEndpointUrl = (db: database) => appUrl.forServer() + appUrl.forDatabaseQuery(db) + endpoints.databases.smuggler.smugglerImport;
        
        

        this.importCommandPowerShell = ko.pureComputed(() => {
            const db = this.activeDatabase();
            if (!db) {
                return "";
            }
            
            const json = modelAsJson();
            const fileName = fileNameProvider();

            return `curl.exe -F 'importOptions=${json.replace(/"/g, '\\"')}' -F 'file=@.\\${fileName}' ${commandEndpointUrl(db)}`;
        });
        
        this.importCommandBash = ko.pureComputed(() => {
            const db = this.activeDatabase();
            if (!db) {
                return "";
            }

            const json = modelAsJson();
            const fileName = fileNameProvider();
            
            return `curl -F 'importOptions=${json}' -F 'file=@${fileName}' ${commandEndpointUrl(db)}`;
        });
        
        this.importCommandCmd = ko.pureComputed(() => {
            const db = this.activeDatabase();
            if (!db) {
                return "";
            }

            const json = modelAsJson();
            const fileName = fileNameProvider();

            return `curl.exe -F "importOptions=${json.replace(/"/g, '\\"')}" -F "file=@.\\${fileName}" ${commandEndpointUrl(db)}`;
        });

        this.isUploading.subscribe((newValue) => {
            if (newValue) {
                this.dirtyFlag().forceDirty();
            } else {
                this.dirtyFlag().reset();
            }
        });

        this.setupValidation();
    }
    
    private setupValidation() {
        this.importedFileName.extend({
            required: true
        });
    }

    attached() {
        super.attached();

        popoverUtils.longWithHover($("#scriptPopover"),
            {
                content:
                    "<div class=\"text-center\">Transform scripts are written in JavaScript </div>" +
                    "<pre><span class=\"token keyword\">var</span> name = <span class=\"token keyword\">this.</span>FirstName;<br />" +
                    "<span class=\"token keyword\">if</span> (name === <span class=\"token string\">'Bob'</span>)<br />&nbsp;&nbsp;&nbsp;&nbsp;" +
                    "<span class=\"token keyword\">throw </span><span class=\"token string\">'skip'</span>; <span class=\"token comment\">// filter-out</span><br /><br />" +
                    "<span class=\"token keyword\">this</span>.Freight = <span class=\"token number\">15.3</span>;<br />" +
                    "</pre>"
            });

        popoverUtils.longWithHover($("#configurationPopover"),
            {
                content:
                    "<div>The following configuration settings will be imported:</div>" +
                    "<strong>Revisions, Expiration & Client Configuration</strong>"
            });
        
        this.updateHelpLink("YD9M1R");
    }

    canDeactivate(isClose: boolean) {
        super.canDeactivate(isClose);

        if (this.isUploading()) {
            this.confirmationMessage("Upload is in progress", "Please wait until uploading is complete.", ["OK"]);
            return false;
        }

        return true;
    }

    createPostboxSubscriptions(): Array<KnockoutSubscription> {
        return [
            ko.postbox.subscribe(EVENTS.ChangesApi.Reconnected, (db: database) => {
                this.isUploading(false);
            })
        ];
    }

    compositionComplete() {
        super.compositionComplete();

        $('[data-toggle="tooltip"]').tooltip();
    }
    
    fileSelected(fileName: string) {
        const isFileSelected = fileName ? !!fileName.trim() : false;
        this.hasFileSelected(isFileSelected);
        this.importedFileName(isFileSelected ? fileName.split(/(\\|\/)/g).pop() : null);
    }

    customizeConfigurationClicked() {
        this.showAdvancedOptions(true);
        this.model.databaseModel.customizeDatabaseRecordTypes(true);

        setTimeout(() => {
            const $customizeRecord = $(".js-customize-record");
            viewHelpers.animate($customizeRecord, "blink-style");

            const topOffset = $customizeRecord.offset().top;
            const container = $customizeRecord.closest(".content-container");
            container.animate({scrollTop: topOffset}, 300);
        }, 200);
    }
    
    importDb() {
        if (!this.isValid(this.validationGroup)) {
            return;
        }

        if (!this.isValid(this.model.validationGroup)) {
            return;
        }

        eventsCollector.default.reportEvent("database", "import");
        this.isUploading(true);

        const fileInput = document.querySelector(importDatabaseFromFile.filePickerTag) as HTMLInputElement;
        const db = this.activeDatabase();
        
        const dtoToValidate = {
            TransformScript: this.model.transformScript()
        } as Raven.Server.Smuggler.Documents.Data.DatabaseSmugglerOptionsServerSide;
        
        new validateSmugglerOptionsCommand(dtoToValidate, db)
            .execute()
            .done(() => {
                this.getNextOperationId(db)
                .done((operationId: number) => {
                    notificationCenter.instance.openDetailsForOperationById(db, operationId);
    
                    this.checkIfRevisionsWasEnabled(db, operationId);
                    
                    new importDatabaseCommand(db, operationId, fileInput.files[0], this.model, this.isUploading, this.uploadStatus)
                        .execute()
                        .always(() => this.isUploading(false));
                });                
            })
            .fail((response: JQueryXHR) => {
                messagePublisher.reportError("Invalid import options", response.responseText, response.statusText);
                this.isUploading(false);
            });
    }
    
    private checkIfRevisionsWasEnabled(db: database, operationId: number) {
        if (!db.hasRevisionsConfiguration()) {
            notificationCenter.instance.databaseOperationsWatch.monitorOperation(operationId)
            .done(() => {
                    new getDatabaseCommand(db.name)
                        .execute()
                        .done(dbInfo => {
                            if (dbInfo.HasRevisionsConfiguration) {
                                db.hasRevisionsConfiguration(true);

                                collectionsTracker.default.configureRevisions(db);
                            }
                        });
                });    
        }
    }

    copyCommandToClipboard(command: string) {
        copyToClipboard.copy(command, "Command was copied to clipboard.");
    }

    private getNextOperationId(db: database): JQueryPromise<number> {
        return new getNextOperationId(db).execute()
            .fail((qXHR, textStatus, errorThrown) => {
                messagePublisher.reportError("Could not get next task id.", errorThrown);
                this.isUploading(false);
            });
    }

}

export = importDatabaseFromFile; 
