import viewModelBase = require("viewmodels/viewModelBase");
import appUrl = require("common/appUrl");
import database = require("models/resources/database");
import eventsCollector = require("common/eventsCollector");
import messagePublisher = require("common/messagePublisher");
import collectionsTracker = require("common/helpers/database/collectionsTracker");
import getExpirationConfigurationCommand = require("commands/database/documents/getExpirationConfigurationCommand");
import saveExpirationConfigurationCommand = require("commands/database/documents/saveExpirationConfigurationCommand");

class expiration extends viewModelBase {

    enabled = ko.observable<boolean>(false);
    specifyDeleteFrequency = ko.observable<boolean>();
    deleteFrequencyInSec = ko.observable<number>();

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
            if (!enabled) {
                this.specifyDeleteFrequency(false);
            }
        })
        
    }
    
    canActivate(args: any) {
        super.canActivate(args);

        const deferred = $.Deferred<canActivateResultDto>();

        this.fetchConfiguration(this.activeDatabase())
            .done(() => deferred.resolve({ can: true }))
            .fail(() => deferred.resolve({ redirect: appUrl.forDatabaseRecord(this.activeDatabase()) }));

        return deferred;
    }


    activate(args: any) {
        super.activate(args);

        this.initValidation();
        
        this.dirtyFlag = new ko.DirtyFlag([this.enabled, this.specifyDeleteFrequency, this.deleteFrequencyInSec]);
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
            }
        });

        this.validationGroup = ko.validatedObservable({
            deleteFrequencyInSec: this.deleteFrequencyInSec
        });
    }

    private fetchConfiguration(db: database): JQueryPromise<Raven.Client.ServerWide.Expiration.ExpirationConfiguration> {
        return new getExpirationConfigurationCommand(db)
            .execute()
            .done((config: Raven.Client.ServerWide.Expiration.ExpirationConfiguration) => {
                this.onConfigurationLoaded(config);
            });
    }


    onConfigurationLoaded(data: Raven.Client.ServerWide.Expiration.ExpirationConfiguration) {
        if (data) {
            this.enabled(data.Active);
            this.specifyDeleteFrequency(data.DeleteFrequencyInSec != null);
            this.deleteFrequencyInSec(data.DeleteFrequencyInSec);

            this.dirtyFlag().reset();
        } else {
            this.enabled(false);
            this.deleteFrequencyInSec(null);
        }
    }
    
    toDto() {
        return {
            Active: this.enabled(),
            DeleteFrequencyInSec: this.specifyDeleteFrequency() ? this.deleteFrequencyInSec() : null
        } as Raven.Client.ServerWide.Expiration.ExpirationConfiguration;
    }

    saveChanges() {
        if (this.isValid(this.validationGroup)) {

            this.spinners.save(true);

            eventsCollector.default.reportEvent("expiration-configuration", "save");

            const dto = this.toDto();

            new saveExpirationConfigurationCommand(this.activeDatabase(), dto)
                .execute()
                .done(() => {
                    this.dirtyFlag().reset();
                    messagePublisher.reportSuccess(`Expiration configuration has been saved`);
                })
                .always(() => {
                    this.spinners.save(false);
                });
        }
    }
    
}

export = expiration;
