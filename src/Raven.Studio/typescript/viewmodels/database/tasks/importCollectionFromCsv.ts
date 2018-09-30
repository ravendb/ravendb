import viewModelBase = require("viewmodels/viewModelBase");
import database = require("models/resources/database");
import messagePublisher = require("common/messagePublisher");
import notificationCenter = require("common/notifications/notificationCenter");
import eventsCollector = require("common/eventsCollector");
import getNextOperationId = require("commands/database/studio/getNextOperationId");
import importFromCsvCommand = require("commands/database/studio/importFromCsvCommand");
import EVENTS = require("common/constants/events");

class importCollectionFromCsv extends viewModelBase {

    private static readonly filePickerTag = "#importCsvFilePicker";

    static isImporting = ko.observable(false);
    isImporting = importCollectionFromCsv.isImporting;

    hasFileSelected = ko.observable(false);
    importedFileName = ko.observable<string>();

    isUploading = ko.observable<boolean>(false);
    uploadStatus = ko.observable<number>();
    
    customCollectionName = ko.observable<string>();

    validationGroup = ko.validatedObservable({
        importedFileName: this.importedFileName,
    });

    constructor() {
        super();

        this.bindToCurrentInstance("fileSelected");

        this.isUploading.subscribe(v => {
            if (!v) {
                this.uploadStatus(0);
            }
        });

        this.setupValidation();
    }

    private setupValidation() {
        this.importedFileName.extend({
            required: true
        });
    }

    createPostboxSubscriptions(): Array<KnockoutSubscription> {
        return [
            ko.postbox.subscribe(EVENTS.ChangesApi.Reconnected, (db: database) => {
                this.isUploading(false);
            })
        ];
    }

    fileSelected(fileName: string) {
        const isFileSelected = fileName ? !!fileName.trim() : false;
        this.hasFileSelected(isFileSelected);
        this.importedFileName(isFileSelected ? fileName.split(/(\\|\/)/g).pop() : null);
    }

    importCsv() {
        if (!this.isValid(this.validationGroup)) {
            return;
        }

        eventsCollector.default.reportEvent("csv", "import");
        this.isUploading(true);

        const fileInput = document.querySelector(importCollectionFromCsv.filePickerTag) as HTMLInputElement;
        const db = this.activeDatabase();

        this.getNextOperationId(db)
            .done((operationId: number) => {
                notificationCenter.instance.openDetailsForOperationById(db, operationId);

                new importFromCsvCommand(db, operationId, fileInput.files[0], this.customCollectionName(), this.isUploading, this.uploadStatus)
                    .execute()
                    .always(() => this.isUploading(false));
            });
    }

    private getNextOperationId(db: database): JQueryPromise<number> {
        return new getNextOperationId(db).execute()
            .fail((qXHR, textStatus, errorThrown) => {
                messagePublisher.reportError("Could not get next task id.", errorThrown);
                this.isUploading(false);
            });
    }

}

export = importCollectionFromCsv; 
