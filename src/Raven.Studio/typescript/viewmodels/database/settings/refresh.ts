import viewModelBase = require("viewmodels/viewModelBase");
import appUrl = require("common/appUrl");
import database = require("models/resources/database");
import eventsCollector = require("common/eventsCollector");
import messagePublisher = require("common/messagePublisher");
import getRefreshConfigurationCommand = require("commands/database/documents/getRefreshConfigurationCommand");
import saveRefreshConfigurationCommand = require("commands/database/documents/saveRefreshConfigurationCommand");

class refresh extends viewModelBase {

    static readonly refreshSample = {
        "Example": "This is an example of a document with @refresh flag set",
        "@metadata": {
            "@collection": "Foo",
            "@refresh": "2017-10-10T08:00:00.0000000Z"
        }
    };
    
    enabled = ko.observable<boolean>(false);
    specifyRefreshFrequency = ko.observable<boolean>();
    refreshFrequencyInSec = ko.observable<number>();
    
    refreshSampleFormatted: string;

    isSaveEnabled: KnockoutComputed<boolean>;
    
    spinners = {
        save: ko.observable<boolean>(false)
    };
    
    validationGroup: KnockoutValidationGroup;

    constructor() {
        super();
        
        this.specifyRefreshFrequency.subscribe(enabled => {
            if (!enabled) {
                this.refreshFrequencyInSec(null);
            }
        });
        
        this.enabled.subscribe(enabled => {
            if (!enabled) {
                this.specifyRefreshFrequency(false);
            }
        });
        
        this.refreshSampleFormatted = Prism.highlight(JSON.stringify(refresh.refreshSample, null, 4), (Prism.languages as any).javascript);
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
        
        this.dirtyFlag = new ko.DirtyFlag([this.enabled, this.specifyRefreshFrequency, this.refreshFrequencyInSec]);
        this.isSaveEnabled = ko.pureComputed<boolean>(() => {
            const dirty = this.dirtyFlag().isDirty();
            const saving = this.spinners.save();
            return dirty && !saving;
        });
    }

    private initValidation() {
        this.refreshFrequencyInSec.extend({
            required: {
                onlyIf: () => this.specifyRefreshFrequency()
            },
            digit: true
        });

        this.validationGroup = ko.validatedObservable({
            deleteFrequencyInSec: this.refreshFrequencyInSec
        });
    }

    private fetchConfiguration(db: database): JQueryPromise<Raven.Client.Documents.Operations.Refresh.RefreshConfiguration> {
        return new getRefreshConfigurationCommand(db)
            .execute()
            .done((config: Raven.Client.Documents.Operations.Refresh.RefreshConfiguration) => {
                this.onConfigurationLoaded(config);
            });
    }

    onConfigurationLoaded(data: Raven.Client.Documents.Operations.Refresh.RefreshConfiguration) {
        if (data) {
            this.enabled(!data.Disabled);
            this.specifyRefreshFrequency(data.RefreshFrequencyInSec != null);
            this.refreshFrequencyInSec(data.RefreshFrequencyInSec);

            this.dirtyFlag().reset();
        } else {
            this.enabled(false);
            this.refreshFrequencyInSec(null);
        }
    }
    
    toDto() : Raven.Client.Documents.Operations.Refresh.RefreshConfiguration {
        return {
            Disabled: !this.enabled(),
            RefreshFrequencyInSec: this.specifyRefreshFrequency() ? this.refreshFrequencyInSec() : null
        };
    }

    saveChanges() {
        if (this.isValid(this.validationGroup)) {
            this.spinners.save(true);

            eventsCollector.default.reportEvent("refresh-configuration", "save");

            const dto = this.toDto();
            const db = this.activeDatabase();

            new saveRefreshConfigurationCommand(db, dto)
                .execute()
                .done(() => {
                    this.dirtyFlag().reset();
                    messagePublisher.reportSuccess(`Refresh configuration has been saved`);
                    db.hasRefreshConfiguration(!dto.Disabled);
                })
                .always(() => {
                    this.spinners.save(false);
                });
        }
    }
}

export = refresh;
