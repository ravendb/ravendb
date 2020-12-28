import viewModelBase = require("viewmodels/viewModelBase");
import appUrl = require("common/appUrl");
import app = require("durandal/app");
import router = require("plugins/router");
import getTimeSeriesStatsCommand = require("commands/database/documents/timeSeries/getTimeSeriesStatsCommand");
import editTimeSeriesEntry = require("viewmodels/database/timeSeries/editTimeSeriesEntry");
import messagePublisher = require("common/messagePublisher");
import virtualGridController = require("widgets/virtualGrid/virtualGridController");
import columnPreviewPlugin = require("widgets/virtualGrid/columnPreviewPlugin");
import getTimeSeriesCommand = require("commands/database/documents/timeSeries/getTimeSeriesCommand");
import textColumn = require("widgets/virtualGrid/columns/textColumn");
import generalUtils = require("common/generalUtils");
import actionColumn = require("widgets/virtualGrid/columns/actionColumn");
import checkedColumn = require("widgets/virtualGrid/columns/checkedColumn");
import deleteTimeSeries = require("viewmodels/database/timeSeries/deleteTimeSeries");
import datePickerBindingHandler = require("common/bindingHelpers/datePickerBindingHandler");
import timeSeriesEntryModel = require("models/database/timeSeries/timeSeriesEntryModel");
import getTimeSeriesConfigurationCommand = require("commands/database/documents/timeSeries/getTimeSeriesConfigurationCommand");
import getDocumentMetadataCommand = require("commands/database/documents/getDocumentMetadataCommand");
import queryUtil = require("common/queryUtil");
import queryCriteria = require("models/database/query/queryCriteria");
import recentQueriesStorage = require("common/storage/savedQueriesStorage");
import popoverUtils = require("common/popoverUtils");

class timeSeriesInfo {
    name = ko.observable<string>();
    numberOfEntries = ko.observable<number>();
    nameAndNumberFormatted: KnockoutComputed<string>;
    
    constructor(name: string, numberOfEntries: number) {
        this.name(name);
        this.numberOfEntries(numberOfEntries);
        
        this.nameAndNumberFormatted = ko.pureComputed(() => {
           return `${this.name()} (${generalUtils.formatNumberToStringFixed(this.numberOfEntries(), 0)})`;
        });
    }
}

class editTimeSeries extends viewModelBase {
    static timeSeriesFormat = "YYYY-MM-DD HH:mm:ss.SSS";
    
    documentId = ko.observable<string>();
    documentCollection = ko.observable<string>();
    
    timeSeriesName = ko.observable<string>();
    timeSeriesNameText: KnockoutComputed<string>;
    timeSeriesList = ko.observableArray<timeSeriesInfo>([]);
    
    urlForDocument: KnockoutComputed<string>;
    canDeleteSelected: KnockoutComputed<boolean>;
    deleteCriteria: KnockoutComputed<timeSeriesDeleteCriteria>;
    deleteButtonText: KnockoutComputed<string>;
    isRollupTimeSeries: KnockoutComputed<boolean>;
    
    isPoliciesDefined = ko.observable<boolean>(false);
    hasMoreThanFiveRawValues = ko.observable<boolean>(false);
        
    namedValuesCache: {[key: string]: Record<string, string[]>} = {};

    private gridController = ko.observable<virtualGridController<Raven.Client.Documents.Session.TimeSeries.TimeSeriesEntry>>();
    private columnPreview = new columnPreviewPlugin<Raven.Client.Documents.Session.TimeSeries.TimeSeriesEntry>();
    
    private columnsCacheInfo = {
        hasTag: false,
        valuesCount: 0
    };
    
    constructor() {
        super();
        
        this.bindToCurrentInstance("changeCurrentSeries", "createTimeSeries", "deleteTimeSeries", "plotTimeSeries", "plotGroupedTimeSeries");
        
        this.initObservables();
        datePickerBindingHandler.install();
    }
    
    canActivate(args: any): JQueryPromise<any> {
        return $.when<any>(super.canActivate(args))
            .then(() => {
                if (!args.name) {
                    return this.activateByCreateNew(args.docId);
                } else {
                    const canActivateResult = $.Deferred<canActivateResultDto>();

                    this.activateById(args.docId, args.name)
                        .done((result) => {
                            if (result.can) {
                                return canActivateResult.resolve({can: true});
                            } else {
                                return canActivateResult.resolve({ redirect: appUrl.forEditDoc(args.docId, this.activeDatabase()) });
                            }
                        })
                        .fail(() => canActivateResult.resolve({ redirect: appUrl.forDocuments(null, this.activeDatabase()) }));

                    return canActivateResult;
                }

                return $.Deferred<canActivateResultDto>().resolve({ can: true });
            });
    }
    
    private getSourceTimeSeriesName(): string {
        if (this.isRollupTimeSeries()) {
            const tsName = this.timeSeriesName();
            const atIdx = tsName.indexOf("@");
            return tsName.substring(0, atIdx);
        } else {
            return this.timeSeriesName();
        }
    }
    
    private getColumnnNamesToUse(columnsCount: number): string[] {
        
        if (this.isRollupTimeSeries()) {
            const definedNamedValues = this.getDefinedNamedValues();
            const aggregationColumnNames = timeSeriesEntryModel.aggregationColumns;
            const aggregationsCount = aggregationColumnNames.length;
            
            if (definedNamedValues) {
                const columnNames = _.range(0, columnsCount)
                    .map(idx => aggregationColumnNames[idx % aggregationsCount] + " (Value #" + Math.floor(idx / aggregationsCount) + ")");
                
                for (let i = 0; i < Math.min(columnsCount, definedNamedValues.length * aggregationsCount); i++) {
                    columnNames[i] = aggregationColumnNames[i % aggregationsCount] + " (" + definedNamedValues[Math.floor(i / aggregationsCount)] + ")";
                }
                return columnNames;
            } else {
                if (columnsCount > aggregationsCount) {
                    return _.range(0, columnsCount)
                        .map(idx => aggregationColumnNames[idx % aggregationsCount] + " (Value #" + Math.floor(idx / aggregationsCount) + ")");
                } else {
                    return aggregationColumnNames;
                }
            }
        } else {
            return this.getValuesNamesToUse(columnsCount);
        }
    }
    
    private getValuesNamesToUse(possibleValuesCount: number, timeSeriesName?: string): string[] {
        const definedNamedValues = this.getDefinedNamedValues(timeSeriesName);
            
        const valuesNamesToUse = _.range(0, possibleValuesCount).map(idx => "Value #" + idx);

        if (definedNamedValues) {
            for (let i = 0; i < Math.min(possibleValuesCount, definedNamedValues.length); i++) {
                valuesNamesToUse[i] = definedNamedValues[i];
            }
        }
        
        return valuesNamesToUse;
    }
    
    private getDefinedNamedValues(timeSeriesName?: string): string[] {
        const collection = this.documentCollection();
        const sourceTimeSeriesName = timeSeriesName || this.getSourceTimeSeriesName();

        let namedValues: string[] = [];
        if (collection && sourceTimeSeriesName) {
            const matchingCollection = Object.keys(this.namedValuesCache)
                .find(x => x.toLocaleLowerCase() === collection.toLocaleLowerCase());

            if (matchingCollection) {
                const perCollectionConfig = this.namedValuesCache[matchingCollection];

                const matchingTimeSeriesConfig = Object.keys(perCollectionConfig)
                    .find(x => x.toLocaleLowerCase() === sourceTimeSeriesName.toLocaleLowerCase());

                if (matchingTimeSeriesConfig) {
                    namedValues = perCollectionConfig[matchingTimeSeriesConfig];
                }
            }
        }
        
        return namedValues;
    }
    
    compositionComplete() {
        super.compositionComplete();
        
        if (!this.timeSeriesName()) {
            this.createTimeSeries(true);
        }
        
        const formatTimeSeriesDate = (input: string) => {
            const dateToFormat = moment.utc(input);
            return dateToFormat.format(editTimeSeries.timeSeriesFormat) + "Z";
        };
        
        const grid = this.gridController();
        grid.headerVisible(true);

        const check = new checkedColumn(true);
        
        const editColumn = new actionColumn<Raven.Client.Documents.Session.TimeSeries.TimeSeriesEntry>(
            grid, item => this.editItem(item), "Edit", `<i class="icon-edit"></i>`, "70px",
            {
                title: () => 'Edit item'
            });

        const timestampColumn = new textColumn<Raven.Client.Documents.Session.TimeSeries.TimeSeriesEntry>(grid, x => formatTimeSeriesDate(x.Timestamp), "Date", "20%");
        
        grid.init((s, t) => this.fetchSeries(s, t).done(result => this.checkColumns(result)), () => {
            const { valuesCount, hasTag } = this.columnsCacheInfo;
            
            const columnNames = this.getColumnnNamesToUse(valuesCount);
            
            const valueColumns = columnNames
                .map((name, idx) => 
                    new textColumn<Raven.Client.Documents.Session.TimeSeries.TimeSeriesEntry>(
                        grid, x => x.Values[idx] ?? "n/a", name, "130px"));
            
            const columns = [ check, editColumn, timestampColumn ];

            columns.push(...valueColumns);
            
            if (hasTag) {
                columns.push(new textColumn<Raven.Client.Documents.Session.TimeSeries.TimeSeriesEntry>(grid, x => x.Tag, "Tag", "20%"));
            }
                        
            return columns;
        });

        this.columnPreview.install("virtual-grid", ".js-time-series-tooltip",
            (item: Raven.Client.Documents.Session.TimeSeries.TimeSeriesEntry, column: textColumn<Raven.Client.Documents.Session.TimeSeries.TimeSeriesEntry>,
             e: JQueryEventObject, onValue: (context: any, valueToCopy?: string) => void) => {
                const value = column.getCellValue && column.getCellValue(item);
                if (column.header === "Edit") {
                    return null;
                } else if (column.header === "Date") {
                    onValue(moment.utc(item.Timestamp).local(), item.Timestamp);
                } else if (!_.isUndefined(value)) {
                    onValue(generalUtils.escapeHtml(value), value);
                }
            });

        this.initTooltips();
    }
    
    private editItem(item: Raven.Client.Documents.Session.TimeSeries.TimeSeriesEntry) {
        const possibleValuesCount = this.isRollupTimeSeries() ? 
            timeSeriesEntryModel.numberOfPossibleRollupValues : 
            timeSeriesEntryModel.numberOfPossibleValues;
        
        const editTimeSeriesEntryDialog = new editTimeSeriesEntry(
            this.documentId(),
            this.activeDatabase(),
            this.timeSeriesName(),
            this.getValuesNamesToUse(possibleValuesCount),
            item
        );
        
        app.showBootstrapDialog(editTimeSeriesEntryDialog)
            .done((seriesName) => {
                if (seriesName) {
                    this.refresh();
                }
            });
    }

    private fetchSeries(skip: number, take: number): JQueryPromise<pagedResult<Raven.Client.Documents.Session.TimeSeries.TimeSeriesEntry>> {
        const fetchTask = $.Deferred<pagedResult<Raven.Client.Documents.Session.TimeSeries.TimeSeriesEntry>>();
        const timeSeriesName = this.timeSeriesName();
        const db = this.activeDatabase();

        if (timeSeriesName) {
            new getTimeSeriesCommand(this.documentId(), timeSeriesName, db, skip, take, true)
                .execute()
                .done(result => {
                    const items = result.Entries;
                    const totalResultCount = result.TotalResults || 0;
                    
                    const series = this.getSeriesFromList(timeSeriesName);
                    if (series) {
                        series.numberOfEntries(result.TotalResults);
                    }

                    fetchTask.resolve({
                        items,
                        totalResultCount
                    })
                })
                .fail((response: JQueryXHR) => {
                    if (response.status === 404) {
                        // looks like all items were deleted - notify user and redirect to document view
                        messagePublisher.reportWarning("Could not find time series: " + timeSeriesName);
                        router.navigate(appUrl.forEditDoc(this.documentId(), db));
                    }
                })
        } else {
            fetchTask.resolve({
                items: [],
                totalResultCount: 0
            })
        }
        
        return fetchTask;
    }
    
    private checkColumns(result: pagedResult<Raven.Client.Documents.Session.TimeSeries.TimeSeriesEntry>) {
        let dirty = false;
        
        if (!this.columnsCacheInfo.hasTag) {
            const hasTag = result.items.find(x => !!x.Tag);
            if (hasTag) {
                this.columnsCacheInfo.hasTag = true;
                dirty = true;
            }
        }
        
        const valuesCount = _.max(result.items.map(x => x.Values.length));
        this.hasMoreThanFiveRawValues(this.isPoliciesDefined() && !this.isRollupTimeSeries() && valuesCount > 5)
        
        if (valuesCount > this.columnsCacheInfo.valuesCount) {
            this.columnsCacheInfo.valuesCount = valuesCount;
            dirty = true;
        }

        if (dirty) {
            this.gridController().markColumnsDirty();
        }
    }

    private refresh(hard = false) {
        this.gridController().reset(hard);
    }

    private activateByCreateNew(docId: string) {
        return this.loadTimeSeries(docId)
            .then(stats => {
                this.timeSeriesList(stats.TimeSeries.map(x => new timeSeriesInfo(x.Name, x.NumberOfEntries)));
                this.timeSeriesName(null);
                return { can: true };
            });
    }
    
    private activateById(docId: string, timeSeriesName: string) {
        return this.loadTimeSeries(docId)
            .then(stats => {
                if (this.getSeriesFromList(timeSeriesName)) {
                    this.timeSeriesName(timeSeriesName);
                    this.timeSeriesList(stats.TimeSeries.map(x => new timeSeriesInfo(x.Name, x.NumberOfEntries)));
                    return { can: true };
                } else {
                    messagePublisher.reportWarning("Unable to find time series with name: " + timeSeriesName);
                    return {can: false };
                }
            });
    }
    
    loadTimeSeries(docId: string) {
        return new getTimeSeriesStatsCommand(docId, this.activeDatabase())
            .execute()
            .done(stats => {
                this.timeSeriesList(stats.TimeSeries.map(x => new timeSeriesInfo(x.Name, x.NumberOfEntries)));
            });
    }

    activate(args: any) {
        super.activate(args);
        
        this.documentId(args.docId);
        
        const tsConfigTask = new getTimeSeriesConfigurationCommand(this.activeDatabase())
            .execute()
            .done(configuration => {
                if (configuration) {
                    this.namedValuesCache = configuration.NamedValues || {};
                    this.isPoliciesDefined(!_.isEmpty(configuration.Collections));
                }
            });
        
        const getDocumentMetadataTask = new getDocumentMetadataCommand(this.documentId(), this.activeDatabase(), true)
            .execute()
            .done(metadata => {
                this.documentCollection(metadata.collection);
            });
        
        return $.when<any>(tsConfigTask, getDocumentMetadataTask);
    }
    
    private cleanColumnsCache() {
        this.columnsCacheInfo = {
            valuesCount: 0,
            hasTag: false
        };
    }
    
    changeCurrentSeries(name: string) {
        this.cleanColumnsCache();
        
        this.timeSeriesName(name);
        
        router.navigate(appUrl.forEditTimeSeries(name, this.documentId(), this.activeDatabase()), false);
        
        this.refresh(true);
    }
    
    deleteTimeSeries() {
        const deleteDialog = new deleteTimeSeries(this.timeSeriesName(), this.documentId(), this.activeDatabase(), this.deleteCriteria());
        app.showBootstrapDialog(deleteDialog)
            .done((postDeleteAction: postTimeSeriesDeleteAction) => {
                switch (postDeleteAction) {
                    case "changeTimeSeries":
                        this.loadTimeSeries(this.documentId())
                            .then((stats) => {
                                const seriesToUse = stats.TimeSeries[0] && stats.TimeSeries[0].Name;
                                if (seriesToUse) {
                                    this.changeCurrentSeries(seriesToUse);
                                } else {
                                    router.navigate(this.urlForDocument());
                                }
                            });
                        break;
                    case "reloadCurrent":
                        this.refresh();
                }
            })
    }
    
    createTimeSeries(createNew: boolean) {
        const tsNameToUse = createNew ? null : this.timeSeriesName();
        const valuesNamesToUse = createNew ? [] : this.getValuesNamesToUse(timeSeriesEntryModel.numberOfPossibleValues);
        
        const createTimeSeriesDialog = new editTimeSeriesEntry(this.documentId(), this.activeDatabase(), tsNameToUse, valuesNamesToUse);
        
        createTimeSeriesDialog.model().name.subscribe((newName) => {
            const valuesNames = this.getValuesNamesToUse(timeSeriesEntryModel.numberOfPossibleValues, newName);
            createTimeSeriesDialog.valuesNames(valuesNames);
        });
        
        app.showBootstrapDialog(createTimeSeriesDialog)
            .done((seriesName) => {
                if (seriesName) {
                    this.onTimeSeriesAdded(seriesName);
                } else if (!this.timeSeriesName()) {
                    // user didn't create new entry, but we requested creation 
                    // redirect back to document
                    router.navigate(this.urlForDocument());
                }
            });
    }

    private onTimeSeriesAdded(seriesName: string) {
        const series = this.getSeriesFromList(seriesName);
        if (series) {
            // New Entry - for an existing time series
            this.changeCurrentSeries(seriesName);
            series.numberOfEntries(series.numberOfEntries() + 1);
        } else {
            // New Time Series - reload data from server
            this.loadTimeSeries(this.documentId())
                .done(() => {
                    this.changeCurrentSeries(seriesName);
                });
        }
    }

    private getSeriesFromList(seriesName: string) {
        return this.timeSeriesList().find(ts => ts.name() === seriesName);
    }

    private initObservables() {
        this.isRollupTimeSeries = ko.pureComputed(() => this.timeSeriesName() && this.timeSeriesName().includes("@"));
        
        this.urlForDocument = ko.pureComputed(() => {
            return appUrl.forEditDoc(this.documentId(), this.activeDatabase());
        });
        
        this.canDeleteSelected = ko.pureComputed(() => {
            const controller = this.gridController();
            if (controller) {
                const selection = controller.selection();

                if (selection.mode === "exclusive" && selection.excluded.length > 0) {
                    try {
                        controller.getSelectedItems();
                        return true;
                    } catch {
                        return false;
                    }
                }

                return true;
            } else {
                return false;
            }
        });
        
        this.deleteCriteria = ko.pureComputed(() => {
            const controller = this.gridController();
            if (!controller) {
                return {
                    mode: "all"
                } as timeSeriesDeleteCriteria;
            }
            const selection = controller.selection();
            
            if (selection.mode === "exclusive") {
                if (selection.excluded.length === 0) {
                    return {
                        mode: "all",
                    } as timeSeriesDeleteCriteria;
                } else {
                    return {
                        mode: "selection",
                        selection: controller.getSelectedItems()
                    } as timeSeriesDeleteCriteria;
                }
            } else {
                if (selection.included.length === 0) {
                    return {
                        mode: "range"
                    } as timeSeriesDeleteCriteria;
                } else {
                    return {
                        mode: "selection",
                        selection: selection.included
                    } as timeSeriesDeleteCriteria;
                }
            }
        });
        
        this.deleteButtonText = ko.pureComputed(() => {
            const criteria = this.deleteCriteria();
            
            switch (criteria.mode) {
                case "all":
                    return "Delete all";
                case "range":
                    return "Delete range";
                case "selection":
                    return "Delete selection";
            }
        });
        
        this.timeSeriesNameText = ko.pureComputed(() => {
            const tsInfo = this.getSeriesFromList(this.timeSeriesName());
            return tsInfo ? tsInfo.nameAndNumberFormatted() : "<creating new>";
        });
    }

    plotTimeSeries() {
        const queryText = queryUtil.formatRawTimeSeriesQuery(this.documentCollection(), this.documentId(), this.timeSeriesName());
        this.plotTimeSeriesByQuery(queryText);
    }

    plotGroupedTimeSeries(group: string) {
        const queryText = queryUtil.formatGroupedTimeSeriesQuery(this.documentCollection(), this.documentId(), this.timeSeriesName(), group);
        this.plotTimeSeriesByQuery(queryText);
    }
    
    plotTimeSeriesByQuery(queryText: string) {
        const query = queryCriteria.empty();
        

        query.queryText(queryText);
        query.name("Time Series: " + this.timeSeriesName() + " (document id: " + this.documentId() + ")");
        query.recentQuery(true);

        const queryDto = query.toStorageDto();
        const recentQueries = recentQueriesStorage.getSavedQueries(this.activeDatabase());
        recentQueriesStorage.appendQuery(queryDto, ko.observableArray(recentQueries));
        recentQueriesStorage.storeSavedQueries(this.activeDatabase(), recentQueries);

        const queryUrl = appUrl.forQuery(this.activeDatabase(), queryDto.hash, "&openGraph=true");
        this.navigate(queryUrl);
    }

    private urlForTimeSeriesPolicies() {
        return appUrl.forTimeSeries(this.activeDatabase());
    }

    private initTooltips() {
        const timeseriesSettingsUrl = this.urlForTimeSeriesPolicies();
        
        popoverUtils.longWithHover($(".raw-data-info"),
            {
                content: `<ul style="max-width: 600px;">
                              <li>
                                  <small>Data below is <strong>Raw Time Series Data</strong>. Entries can be edited as needed.</small>
                              </li>
                              <li>
                                  <small> The raw data can be aggregated per time period that is configured in the <a href="${timeseriesSettingsUrl}">Time Series Settings</a> view.</small>
                              </li>
                          </ul>`,
                placement: "right",
                html: true,
                container: ".edit-time-series"
            });

        popoverUtils.longWithHover($(".rollups-info"),
            {
                content: `<br>
                          <ul style="max-width: 700px;">
                              <li>
                                  <small>Data below is not raw data.</small><br />
                                  <small>Each entry is <strong>Rolled-Up Data</strong> that is aggregated for a <strong>specific time frame</strong> defined by a <a href="${timeseriesSettingsUrl}">Time Series Policy</a>.</small>
                              </li>
                              <li>
                                  <small>The rolled-up data is only available when the aggregation time frame defined by the policy has ended.</small>
                              </li>
                          </ul>`,
                placement: "right",
                html: true,
                container: ".edit-time-series"
            })
    }
}

export = editTimeSeries;
