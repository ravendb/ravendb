import appUrl = require("common/appUrl");
import database = require("models/resources/database");
import saveTimeSeriesConfigurationCommand = require("commands/database/documents/timeSeries/saveTimeSeriesConfigurationCommand");
import eventsCollector = require("common/eventsCollector");
import generalUtils = require("common/generalUtils");
import messagePublisher = require("common/messagePublisher");
import collectionsTracker = require("common/helpers/database/collectionsTracker");
import getTimeSeriesConfigurationCommand = require("commands/database/documents/timeSeries/getTimeSeriesConfigurationCommand");
import timeSeriesConfigurationEntry = require("models/database/documents/timeSeriesConfigurationEntry");
import popoverUtils = require("common/popoverUtils");
import shardViewModelBase from "viewmodels/shardViewModelBase";
import licenseModel from "models/auth/licenseModel";
import { TimeSeriesInfoHub } from "viewmodels/database/settings/TimeSeriesInfoHub";

class timeSeries extends shardViewModelBase {

    view = require("views/database/settings/timeSeries.html");

    perCollectionConfigurations = ko.observableArray<timeSeriesConfigurationEntry>([]);
    isSaveEnabled: KnockoutComputed<boolean>;
    collections = collectionsTracker.default.collections;
    selectionState: KnockoutComputed<checkbox>;
    selectedItems = ko.observableArray<timeSeriesConfigurationEntry>([]);
    
    policyCheckFrequency = ko.observable<number>(10 * 60);

    currentlyEditedItem = ko.observable<timeSeriesConfigurationEntry>(); // reference to cloned and currently being edited item
    currentBackingItem = ko.observable<timeSeriesConfigurationEntry>(null); // original item which is edited

    spinners = {
        save: ko.observable<boolean>(false)
    };

    hasTimeSeriesRollupsAndRetention = licenseModel.getStatusValue("HasTimeSeriesRollupsAndRetention");
    infoHubView: ReactInKnockout<typeof TimeSeriesInfoHub>;

    constructor(db: database) {
        super(db);

        this.bindToCurrentInstance("saveChanges",
            "deleteItem", "editItem", "applyChanges",
            "exitEditMode", "enableConfiguration",
            "disableConfiguration", "toggleSelectAll");

        this.initObservables();

        this.infoHubView = ko.pureComputed(() => ({
            component: TimeSeriesInfoHub
        }))
    }

    private initObservables() {
        this.selectionState = ko.pureComputed<checkbox>(() => {
            const selectedCount = this.selectedItems().length;
            const totalCount = this.perCollectionConfigurations().length;
            if (totalCount && selectedCount === totalCount)
                return "checked";
            if (selectedCount > 0)
                return "some_checked";
            return "unchecked";
        });
    }

    canActivate(args: any): boolean | JQueryPromise<canActivateResultDto> {
        return $.when<any>(super.canActivate(args))
            .then(() => {
                const deferred = $.Deferred<canActivateResultDto>();

                this.fetchConfiguration(this.db)
                    .done(() => deferred.resolve({ can: true }))
                    .fail(() => deferred.resolve({ redirect: appUrl.forDatabaseRecord(this.db) }));

                return deferred;
            });
    }

    activate(args: any) {
        super.activate(args);

        this.dirtyFlag = new ko.DirtyFlag([this.perCollectionConfigurations, this.policyCheckFrequency, this.currentlyEditedItem]);
        this.isSaveEnabled = ko.pureComputed<boolean>(() => {
            const dirty = this.dirtyFlag().isDirty();
            const saving = this.spinners.save();
            return dirty && !saving;
        });
    }

    private fetchConfiguration(db: database): JQueryPromise<Raven.Client.Documents.Operations.TimeSeries.TimeSeriesConfiguration> {
        return new getTimeSeriesConfigurationCommand(db)
            .execute()
            .done((config: Raven.Client.Documents.Operations.TimeSeries.TimeSeriesConfiguration) => {
                this.onConfigurationLoaded(config);
            });
    }

    onConfigurationLoaded(data: Raven.Client.Documents.Operations.TimeSeries.TimeSeriesConfiguration) {
        if (data) {
            this.policyCheckFrequency(generalUtils.timeSpanToSeconds(data.PolicyCheckFrequency));
            if (data.Collections) {
                this.perCollectionConfigurations(Object.entries(data.Collections ?? []).map(([collection, configuration]) => {
                    const entry = new timeSeriesConfigurationEntry(collection);
                    entry.withRetention(configuration);
                    return entry;
                }));
            }

            if (data.NamedValues) {
                Object.entries(data.NamedValues).map(([collection, configuration]) => {
                    let matchingItem = this.perCollectionConfigurations().find(x => x.collection() === collection);
                    if (!matchingItem) {
                        matchingItem = new timeSeriesConfigurationEntry(collection);
                        this.perCollectionConfigurations.push(matchingItem);
                    }
                    
                    matchingItem.withNamedValues(configuration);
                });
            }
            
            this.dirtyFlag().reset();
        } else {
            this.perCollectionConfigurations([]);
        }
    }

    createCollectionNameAutocompleter(item: timeSeriesConfigurationEntry) {
        return ko.pureComputed(() => {
            const key = item.collection(); 
            const options = collectionsTracker.default.getCollectionNames();
            
            const usedOptions = this.perCollectionConfigurations().filter(f => f !== item).map(x => x.collection());

            const filteredOptions = options.filter(x => !usedOptions.includes(x));

            if (key) {
                return filteredOptions.filter(x => x.toLowerCase().includes(key.toLowerCase()));
            } else {
                return filteredOptions;
            }
        });
    }

    addCollectionSpecificConfiguration() {
        eventsCollector.default.reportEvent("timeSeriesSettings", "create");

        this.currentBackingItem(null);
        this.currentlyEditedItem(timeSeriesConfigurationEntry.empty());

        this.currentlyEditedItem().validationGroup.errors.showAllMessages(false);
        this.initTooltips();
    }
    
    private initTooltips() {
        popoverUtils.longWithHover($(".aggregation-info"),
            {
                content: `
                         <small>
                             <ul class="margin-top-sm">
                                 <li class="margin-bottom">
                                     <strong>A new time series</strong> is created by each Rollup Policy defined.<br>
                                     Its name will be <code>&lt;Raw-data-time-series-name&gt;@&lt;Rollup-policy-name&gt;</code>
                                  </li>
                                  <li class="margin-bottom">
                                     Each entry in such auto-generated time series has the following values:<br>
                                     <i>First, Last, Min, Max, Sum, Count</i> (per each original raw value)<br>
                                     which is rolled-up data <strong>based only on the Aggregation Time Frame defined by the policy!</strong>
                                 </li>
                                 <li class="margin-bottom">
                                     The data generated by the first Rollup Policy is based on the Raw time series data.<br>
                                     After that, input data for each Rollup Policy is the data generated by the previous policy.
                                 </li>
                                 <li>
                                     <strong>Important</strong>:<br>
                                     Rollup entries are not created for time series that have entries with more than 5 values!<br>
                                     (even if policy is defined)
                                 </li>
                              </ul>
                         </small>
                         `,
                placement: "left",
                html: true,
                container: ".time-series-config"
            });
        
        popoverUtils.longWithHover($(".named-values-info"),
            {
                content: `
                        <small>
                            <ul class="margin-top-sm">
                                <li class="margin-bottom"><strong>Associate names with your time series values</strong>.
                                </li>
                                <li class="margin-bottom">The default name per value is the value's position (i.e. <code>Value #0</code>)<br>
                                    Instead, values names can be customized.
                                </li>
                                <li class="margin-bottom"><strong>For example</strong>: If time series tracks stock exchange data,<br>
                                    the following names can be assigned: Open, Close, High, Low, Volume.
                                </li>
                            </ul>
                        </small>
                        `,
                placement: "left",
                html: true,
                container: ".time-series-config"
            });
    }

    removeConfiguration(entry: timeSeriesConfigurationEntry) {
        eventsCollector.default.reportEvent("timeSeriesSettings", "remove");

        this.perCollectionConfigurations.remove(entry);
    }

    applyChanges() {
        const itemToSave = this.currentlyEditedItem();
        const isEdit = !!this.currentBackingItem();
        
        let hasErrors = false;
        
        if (!this.isValid(itemToSave.validationGroup)) {
            hasErrors = true;
        }
        
        if (!this.isValid(itemToSave.rawPolicy().validationGroup)) {
            hasErrors = true;
        }

        for (const namedValues of itemToSave.namedValues()) {
            if (!this.isValid(namedValues.validationGroup)) {
                hasErrors = true;
            }
            
            for (const namedValue of namedValues.namedValues()) {
                if (!this.isValid(namedValue.validationGroup)) {
                    hasErrors = true;
                }
            }
        }

        for (const policy of itemToSave.policies()) {
            if (!this.isValid(policy.validationGroup)) {
                hasErrors = true;
            }
        }

        if (hasErrors) {
            return;
        }

        if (isEdit) {
            this.currentBackingItem().copyFrom(itemToSave);
        } else {
            this.perCollectionConfigurations.push(itemToSave);
        }

        this.exitEditMode();
    }

    toDto(): Raven.Client.Documents.Operations.TimeSeries.TimeSeriesConfiguration {
        const perCollectionConfigurations = this.perCollectionConfigurations();

        const collectionsDto = {} as { [key: string]: Raven.Client.Documents.Operations.TimeSeries.TimeSeriesCollectionConfiguration; };
        const namedValuesDto = {} as {[key: string]: System.Collections.Generic.Dictionary<string, string[]>;};

        perCollectionConfigurations.forEach(config => {
            if (config.hasPoliciesOrRetentionConfig()) {
                collectionsDto[config.collection()] = config.toPoliciesDto();
            }
            
            if (config.hasNamedValuesConfig()) {
                namedValuesDto[config.collection()] = config.toNamedValuesDto();
            }
        });

        return {
            Collections: collectionsDto,
            PolicyCheckFrequency: this.policyCheckFrequency() ? generalUtils.formatAsTimeSpan(this.policyCheckFrequency() * 1000) : null,
            NamedValues : namedValuesDto
        }
    }

    saveChanges() {
        // first apply current changes:
        const itemBeingEdited = this.currentlyEditedItem();
        if (itemBeingEdited) {
            if (this.isValid(itemBeingEdited.validationGroup)) {
                this.applyChanges();
                
                if (this.currentlyEditedItem()) {
                    // looks like we didn't exit edit mode - validation errors?
                    return;
                }
            } else {
                // we have validation error - stop saving
                return;
            }
        }

        this.spinners.save(true);

        eventsCollector.default.reportEvent("timeSeriesSettings", "save");

        const dto = this.toDto();

        new saveTimeSeriesConfigurationCommand(this.db, dto)
            .execute()
            .done(() => {
                this.dirtyFlag().reset();
                messagePublisher.reportSuccess(`Time series configuration has been saved`);
            })
            .always(() => {
                this.spinners.save(false);
                const db = this.db;
                db.hasRevisionsConfiguration(true);
                collectionsTracker.default.configureRevisions(db);
            });
    }

    editItem(entry: timeSeriesConfigurationEntry) {
        this.currentBackingItem(entry);
        const clone = timeSeriesConfigurationEntry.empty().copyFrom(entry);
        this.currentlyEditedItem(clone);

        this.initTooltips();
    }

    deleteItem(entry: timeSeriesConfigurationEntry) {
        this.selectedItems.remove(entry);

        this.perCollectionConfigurations.remove(entry);

        this.exitEditMode();
    }

    exitEditMode() {
        this.currentBackingItem(null);
        this.currentlyEditedItem(null);
    }

    enableConfiguration(entry: timeSeriesConfigurationEntry) {
        entry.disabled(false);
    }

    disableConfiguration(entry: timeSeriesConfigurationEntry) {
        entry.disabled(true);
    }

    enableSelected() {
        this.selectedItems().forEach(item => item.disabled(false));
    }

    disableSelected() {
        this.selectedItems().forEach(item => item.disabled(true));
    }

    toggleSelectAll() {
        eventsCollector.default.reportEvent("timeSeries", "toggle-select-all");
        const selectedCount = this.selectedItems().length;

        if (selectedCount > 0) {
            this.selectedItems([]);
        } else {
            const selectedItems = this.perCollectionConfigurations().slice();

            this.selectedItems(selectedItems);
        }
    }
}

export = timeSeries;
