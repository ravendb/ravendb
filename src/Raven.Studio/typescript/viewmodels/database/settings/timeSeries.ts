import viewModelBase = require("viewmodels/viewModelBase");
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

class timeSeries extends viewModelBase {

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

    constructor() {
        super();

        this.bindToCurrentInstance("saveChanges",
            "deleteItem", "editItem", "applyChanges",
            "exitEditMode", "enableConfiguration",
            "disableConfiguration", "toggleSelectAll");

        this.initObservables();
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

                this.fetchConfiguration(this.activeDatabase())
                    .done(() => deferred.resolve({ can: true }))
                    .fail(() => deferred.resolve({ redirect: appUrl.forDatabaseRecord(this.activeDatabase()) }));

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
                this.perCollectionConfigurations(_.map(data.Collections, (configuration, collection) => {
                    const entry = new timeSeriesConfigurationEntry(collection);
                    entry.withRetention(configuration);
                    return entry;
                }));
            }

            if (data.NamedValues) {
                _.map(data.NamedValues, (configuration, collection) => {
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

            const filteredOptions = _.difference(options, usedOptions);

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
                content: `<ol style="max-width: 400px;">
                                    <li>
                                        <small>
                                            <strong>New time-series data</strong> is created for each aggregation policy that is defined.
                                            <br />
                                            Its name will be &lt;original-time-series-name&gt;_&lt;additional-policy-name&gt;
                                        </small>
                                    </li>
                                    <li>
                                        <small>
                                            The time-series data generated by the <strong>first</strong> aggregation policy is based on the Raw time-series data.
                                            After that, the input data for each a aggregation policy will be the time-series data that was generated by the <strong>previous</strong> policy. <br />
                                            The first Aggregation Rule is based on the Time Series data.<br />
                                            Each of the following Aggregation Rules is based on data from its <strong>preceding</strong> rule.
                                        </small>
                                    </li>
                                </ol>`,
                placement: "left",
                html: true,
                container: ".time-series-config"
            });
        
        popoverUtils.longWithHover($(".named-values-info"), 
            {
                content: `
                        <small>
                            Named values allow to associate names with time series values.<br /> 
                            Instead of using position (<code>Values[0]</code>) strong name can be used. (<code>Volume</code>)<br />
                            Example: If time series track stock exchange, the following names can be assigned: <br />
                            Open, Close, High, Low, Volume
                        </small>
                        `,
                placement: "left",
                html: true,
                container: ".time-series-config"
            })
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

        for (let namedValues of itemToSave.namedValues()) {
            if (!this.isValid(namedValues.validationGroup)) {
                hasErrors = true;
            }
            
            for (let namedValue of namedValues.namedValues()) {
                if (!this.isValid(namedValue.validationGroup)) {
                    hasErrors = true;
                }
            }
        }

        for (let policy of itemToSave.policies()) {
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
            if (config.hasRetentionConfig()) {
                collectionsDto[config.collection()] = config.toRetentionDto();    
            }
            
            if (config.hasNamedValuesConfig()) {
                namedValuesDto[config.collection()] = config.toNamedValuesDto();
            }
        });

        return {
            Collections: collectionsDto,
            PolicyCheckFrequency: generalUtils.formatAsTimeSpan(this.policyCheckFrequency() * 1000),
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

        new saveTimeSeriesConfigurationCommand(this.activeDatabase(), dto)
            .execute()
            .done(() => {
                this.dirtyFlag().reset();
                messagePublisher.reportSuccess(`Time series configuration has been saved`);
            })
            .always(() => {
                this.spinners.save(false);
                const db = this.activeDatabase();
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
