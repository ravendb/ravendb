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
import generalUtils = require("common/generalUtils");

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
    isBackupFileType: KnockoutComputed<boolean>;
    isSnapshotFileType: KnockoutComputed<boolean>;

    isUploading = ko.observable<boolean>(false);
    uploadStatus = ko.observable<number>();
    
    commandTypes: Array<commandLineType> = ["PowerShell", "Cmd", "Bash"];
    effectiveCommandType = ko.observable<commandLineType>("PowerShell");
    effectiveCommand: KnockoutComputed<string>;
    
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
        
        this.effectiveCommand = ko.pureComputed(() => {
            return this.getCommand(this.effectiveCommandType());
        });

        this.isUploading.subscribe((newValue) => {
            if (newValue) {
                this.dirtyFlag().forceDirty();
            } else {
                this.dirtyFlag().reset();
            }
        });

        this.isBackupFileType = ko.pureComputed(() => {
            if (!this.hasFileSelected() || !this.importedFileName()) {
                return false;
            }
            
            const fileExtension = generalUtils.getFileExtension(this.importedFileName());
            return _.includes(["ravendb-full-backup", "ravendb-encrypted-full-backup", "ravendb-incremental-backup", "ravendb-encrypted-incremental-backup"], fileExtension);
        });

        this.isSnapshotFileType = ko.pureComputed(() => {
            if (!this.hasFileSelected() || !this.importedFileName()) {
                return false;
            }

            const fileExtension = generalUtils.getFileExtension(this.importedFileName());
            return _.includes(["ravendb-snapshot", "ravendb-encrypted-snapshot"], fileExtension);
        });
        
        this.setupValidation();
    }
    
    private setupValidation() {
        this.importedFileName.extend({
            required: true,
            validation: [
                {
                    validator: () => !this.isSnapshotFileType(),
                    message: "The selected file is a RavenDB Snapshot file and cannot be imported. " +
                             "Use the 'Restore' option (under Create New Database) in order to restore data from a RavenDB Snapshot file."
                }
            ]
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

        popoverUtils.longWithHover($("#js-ongoing-tasks-disabled"), {
            content: "Imported ongoing tasks will be disabled by default."
        });

        popoverUtils.longWithHover($("#js-import-artificial-documents"), {
            content: "Importing artificial documents might cause import error of Map-Reduce indexes with OutputReduceToCollection."
        });
        
        this.updateHelpLink("YD9M1R");
    }

    canDeactivate(isClose: boolean) {
        super.canDeactivate(isClose);

        if (this.isUploading()) {
            this.confirmationMessage("Upload is in progress", "Please wait until uploading is complete.", {
                buttons: ["OK"]
            });
            return false;
        }

        return true;
    }

    createPostboxSubscriptions(): Array<KnockoutSubscription> {
        return [
            ko.postbox.subscribe(EVENTS.ChangesApi.Reconnected, () => {
                this.isUploading(false);
            })
        ];
    }

    compositionComplete() {
        super.compositionComplete();

        $('[data-toggle="tooltip"]').tooltip();
        this.model.databaseModel.init();
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

    private getNextOperationId(db: database): JQueryPromise<number> {
        return new getNextOperationId(db).execute()
            .fail((qXHR, textStatus, errorThrown) => {
                messagePublisher.reportError("Could not get next task id.", errorThrown);
                this.isUploading(false);
            });
    }

    getCommandTypeLabel(cmdType: commandLineType) {
        return `Import Command - ${cmdType}`;
    }

    copyCommandToClipboard() {
        const command = this.effectiveCommand();
        copyToClipboard.copy(command, "Import command was copied to clipboard.");
    }
    
    private getCommand(commandType: commandLineType) {

        const db = this.activeDatabase();
        if (!db) {
            return "";
        }

        const args = this.model.toDto();
        if (!args.TransformScript) {
            delete args.TransformScript;
        }

        const json = JSON.stringify(args);
        const fileName = this.importedFileName() || "Dump of Database.ravendbdump";
        const commandEndpointUrl = (db: database) => appUrl.forServer() + appUrl.forDatabaseQuery(db) + endpoints.databases.smuggler.smugglerImport;
        
        switch (commandType) {
            case "PowerShell":
                return `curl.exe -F 'importOptions=${json.replace(/"/g, '\\"')}' -F 'file=@.\\${fileName}' ${commandEndpointUrl(db)}`;
            case "Cmd":
                return `curl.exe -F "importOptions=${json.replace(/"/g, '\\"')}" -F "file=@.\\${fileName}" ${commandEndpointUrl(db)}`;
            case "Bash":
                return `curl -F 'importOptions=${json}' -F 'file=@${fileName}' ${commandEndpointUrl(db)}`;
        }
    }
}

export = importDatabaseFromFile; 
