import viewModelBase = require("viewmodels/viewModelBase");
import database = require("models/resources/database");
import messagePublisher = require("common/messagePublisher");
import notificationCenter = require("common/notifications/notificationCenter");
import eventsCollector = require("common/eventsCollector");
import getNextOperationId = require("commands/database/studio/getNextOperationId");
import importFromCsvCommand = require("commands/database/studio/importFromCsvCommand");
import EVENTS = require("common/constants/events");

type delimiters = "Comma" | "Semicolon" | "Tab" | "Space";
type quoteChars = "Double quote" | "Single quote";
type trimOptions = "No trimming" | "Trim whitespace around fields" | "Trim whitespace inside strings";

class csvConfiguration {
   
    static readonly possibleDelimiters: Array<delimiters> = ["Comma", "Semicolon", "Tab", "Space"];
    static readonly possibleQuoteChars: Array<quoteChars> = ["Double quote", "Single quote"];
    static readonly possibleTrimOptions: Array<trimOptions> = ["No trimming", "Trim whitespace around fields", "Trim whitespace inside strings"];

    delimiter = ko.observable<delimiters>("Comma");
    quote = ko.observable<quoteChars>("Double quote");
    trimOption = ko.observable<trimOptions>("No trimming");
    
    allowComments = ko.observable<boolean>(false);
    commentCharacter = ko.observable<string>();
    
    toDto(): Raven.Server.Smuggler.Documents.CsvImportOptions {
        return {
            Delimiter: this.getDelimiter(),
            Quote: this.getQuote(),
            TrimOptions: this.getTrimOption(),
            AllowComments: this.allowComments(),
            Comment: _.trim(this.commentCharacter()) || undefined 
        }
    }

    getDelimiter(): string {
        switch (this.delimiter()) {
            case "Comma": 
                return ",";
            case "Semicolon": 
                return ";";
            case "Tab": 
                return "\t";
            case "Space": 
                return " ";
        }
    }

    getQuote(): string {
        switch (this.quote()) {
            case "Double quote": 
                return '"';
            case "Single quote": 
                return "'";
        }
    }

    getTrimOption(): CsvHelper.Configuration.TrimOptions {
        switch (this.trimOption()) {
            case "No trimming": 
                return "None";
            case "Trim whitespace around fields": 
                return "Trim";
            case "Trim whitespace inside strings": 
                return "InsideQuotes";
        }
    }
}

class importCollectionFromCsv extends viewModelBase {

    view = require("views/database/tasks/importCollectionFromCsv.html");
   
    private static readonly filePickerTag = "#importCsvFilePicker";
    csvConfig: csvConfiguration = new csvConfiguration();

    static isImporting = ko.observable(false);
    isImporting = importCollectionFromCsv.isImporting;

    hasFileSelected = ko.observable(false);
    importedFileName = ko.observable<string>();

    isUploading = ko.observable<boolean>(false);
    uploadStatus = ko.observable<number>();
    
    customCollectionName = ko.observable<string>();

    validationGroup = ko.validatedObservable({
        importedFileName: this.importedFileName,
        commentCharacter: this.csvConfig.commentCharacter
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
        
        this.csvConfig.commentCharacter.extend({
            required: {
                onlyIf: (val: string) => this.csvConfig.allowComments() &&  _.trim(val) === ""
            },
            
            validation: [
                {
                    validator: (val: string) => !this.csvConfig.allowComments() || (val && _.trim(val).length === 1),
                    message: 'Please enter only one character for the comment character'
                }
            ]
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

                new importFromCsvCommand(db, operationId, fileInput.files[0], this.customCollectionName(), this.isUploading, this.uploadStatus, this.csvConfig.toDto())
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
