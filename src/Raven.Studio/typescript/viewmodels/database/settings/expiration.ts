import viewModelBase = require("viewmodels/viewModelBase");
import appUrl = require("common/appUrl");
import database = require("models/resources/database");
import eventsCollector = require("common/eventsCollector");
import messagePublisher = require("common/messagePublisher");
import getExpirationConfigurationCommand = require("commands/database/documents/getExpirationConfigurationCommand");
import saveExpirationConfigurationCommand = require("commands/database/documents/saveExpirationConfigurationCommand");
import { highlight, languages } from "prismjs";

class expiration extends viewModelBase {

    view = require("views/database/settings/expiration.html");

    static readonly expirationSample = {
        "Example": "This is an example of a document with @expires flag set",
        "@metadata": {
            "@collection": "Foo",
            "@expires": "2017-10-10T08:00:00.0000000Z"
        }
    };
    
    static readonly defaultItemsToProcess = 65536;
    
    enabled = ko.observable<boolean>(false);
    specifyDeleteFrequency = ko.observable<boolean>();
    deleteFrequencyInSec = ko.observable<number>();
    
    limitMaxItemsToProcess = ko.observable<boolean>();
    maxItemsToProcess = ko.observable<number>();
    
    expirationSampleFormatted: string;

    isSaveEnabled: KnockoutComputed<boolean>;
    
    spinners = {
        save: ko.observable<boolean>(false)
    };
    
    validationGroup: KnockoutValidationGroup;

    constructor() {
        super();
        
        this.specifyDeleteFrequency.subscribe(enabled => {
            if (!enabled) {
                this.deleteFrequencyInSec(null);
            }
        });
        
        this.enabled.subscribe(enabled => {
            if (enabled) {
                this.limitMaxItemsToProcess(true);
            } else {
                this.specifyDeleteFrequency(false);
                this.limitMaxItemsToProcess(false);
            }
        });
        
        this.limitMaxItemsToProcess.subscribe(hasLimit => {
            if (hasLimit) {
                this.maxItemsToProcess(expiration.defaultItemsToProcess);
            } else {
                this.maxItemsToProcess(null);
            }
        })
        
        this.expirationSampleFormatted = highlight(JSON.stringify(expiration.expirationSample, null, 4), languages.javascript, "js");
    }
    
    canActivate(args: any) {
        return $.when<any>(super.canActivate(args))
            .then(() => {
                const deferred = $.Deferred<canActivateResultDto>();

                this.fetchConfiguration(this.activeDatabase())
                    .done(() => deferred.resolve({ can: true }))
                    .fail(() => deferred.resolve({ redirect: appUrl.forDatabaseRecord(this.activeDatabase()) }));

                return deferred;
            });
    }

    activate(args: any) {
        super.activate(args);

        this.initValidation();
        
        this.dirtyFlag = new ko.DirtyFlag([this.enabled, this.specifyDeleteFrequency, this.deleteFrequencyInSec, this.limitMaxItemsToProcess, this.maxItemsToProcess]);
        this.isSaveEnabled = ko.pureComputed<boolean>(() => {
            const dirty = this.dirtyFlag().isDirty();
            const saving = this.spinners.save();
            return dirty && !saving;
        });
    }

    private initValidation() {
        this.deleteFrequencyInSec.extend({
            required: {
                onlyIf: () => this.specifyDeleteFrequency()
            },
            digit: true
        });
        this.maxItemsToProcess.extend({
            required: {
                onlyIf: () => this.limitMaxItemsToProcess()
            },
            digit: true
        });

        this.validationGroup = ko.validatedObservable({
            deleteFrequencyInSec: this.deleteFrequencyInSec,
            maxItemsToProcess: this.maxItemsToProcess
        });
    }

    private fetchConfiguration(db: database): JQueryPromise<Raven.Client.Documents.Operations.Expiration.ExpirationConfiguration> {
        return new getExpirationConfigurationCommand(db)
            .execute()
            .done((config: Raven.Client.Documents.Operations.Expiration.ExpirationConfiguration) => {
                this.onConfigurationLoaded(config);
            });
    }

    onConfigurationLoaded(data: Raven.Client.Documents.Operations.Expiration.ExpirationConfiguration) {
        if (data) {
            this.enabled(!data.Disabled);
            this.specifyDeleteFrequency(data.DeleteFrequencyInSec != null);
            this.deleteFrequencyInSec(data.DeleteFrequencyInSec);
            
            this.limitMaxItemsToProcess(data.MaxItemsToProcess != null);
            this.maxItemsToProcess(data.MaxItemsToProcess);

            this.dirtyFlag().reset();
        } else {
            this.enabled(false);
            this.deleteFrequencyInSec(null);
        }
    }
    
    toDto() : Raven.Client.Documents.Operations.Expiration.ExpirationConfiguration {
        return {
            Disabled: !this.enabled(),
            DeleteFrequencyInSec: this.specifyDeleteFrequency() ? this.deleteFrequencyInSec() : null,
            MaxItemsToProcess: this.limitMaxItemsToProcess() ? this.maxItemsToProcess() : null,
        };
    }

    saveChanges() {
        if (this.isValid(this.validationGroup)) {
            this.spinners.save(true);

            eventsCollector.default.reportEvent("expiration-configuration", "save");

            const dto = this.toDto();
            const db = this.activeDatabase();

            new saveExpirationConfigurationCommand(db, dto)
                .execute()
                .done(() => {
                    this.dirtyFlag().reset();
                    messagePublisher.reportSuccess(`Expiration configuration has been saved`);
                    db.hasExpirationConfiguration(!dto.Disabled);
                })
                .always(() => {
                    this.spinners.save(false);
                });
        }
    }
}

export = expiration;
