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
import timeSeriesModel = require("models/database/timeSeries/timeSeriesModel");
import getTimeSeriesConfigurationCommand = require("commands/database/documents/timeSeries/getTimeSeriesConfigurationCommand");
import getDocumentMetadataCommand = require("commands/database/documents/getDocumentMetadataCommand");

class editTimeSeries extends viewModelBase {
    static timeSeriesFormat = "YYYY-MM-DD HH:mm:ss.SSS";
    
    documentId = ko.observable<string>();
    documentCollection = ko.observable<string>();
    timeSeriesName = ko.observable<string>();
    timeSeriesNames = ko.observableArray<string>([]);
    
    urlForDocument: KnockoutComputed<string>;
    canDeleteSelected: KnockoutComputed<boolean>;
    deleteCriteria: KnockoutComputed<timeSeriesDeleteCriteria>;
    deleteButtonText: KnockoutComputed<string>;
    isAggregation: KnockoutComputed<boolean>;
    
    namedValuesCache: {[key: string]: Record<string, string[]>} = {};

    private gridController = ko.observable<virtualGridController<Raven.Client.Documents.Session.TimeSeries.TimeSeriesEntry>>();
    private columnPreview = new columnPreviewPlugin<Raven.Client.Documents.Session.TimeSeries.TimeSeriesEntry>();
    
    private columnsCacheInfo = {
        hasTag: false,
        valuesCount: 0
    };
    
    constructor() {
        super();
        
        this.bindToCurrentInstance("changeCurrentSeries", "createTimeSeries", "deleteTimeSeries");
        
        this.initObservables();
        datePickerBindingHandler.install();
    }
    
    canActivate(args: any): JQueryPromise<any> {
        return $.when<any>(super.canActivate(args))
            .then(() => {
                if (!args.name) {
                    return this.activateByCreateNew(args.docId);
                } else {
                    return this.activateById(args.docId, args.name);    
                }
            });
    }
    
    private getSourceTimeSeriesName(): string {
        if (this.isAggregation()) {
            const tsName = this.timeSeriesName();
            const atIdx = tsName.indexOf("@");
            return tsName.substring(0, atIdx);
        } else {
            return this.timeSeriesName();
        }
    }
    
    private getValueColumnNames(valuesCount: number): string[] {
        const collection = this.documentCollection();
        const sourceTimeSeriesName = this.getSourceTimeSeriesName();
        
        let namedColumns: string[];
        if (collection && sourceTimeSeriesName) {
            const matchingCollection = Object.keys(this.namedValuesCache)
                .find(x => x.toLocaleLowerCase() === collection.toLocaleLowerCase());
            
            if (matchingCollection) {
                const perCollectionConfig = this.namedValuesCache[matchingCollection];
                
                const matchingTimeSeriesConfig = Object.keys(perCollectionConfig)
                    .find(x => x.toLocaleLowerCase() === sourceTimeSeriesName.toLocaleLowerCase());
                
                if (matchingTimeSeriesConfig) {
                    namedColumns = perCollectionConfig[matchingTimeSeriesConfig];
                }
            }
        }
        
        if (this.isAggregation()) {
            const aggregationColumnNames = timeSeriesModel.aggregationColumns;
            const aggregationsCount = aggregationColumnNames.length;
            
            if (namedColumns) {
                const columnNames = _.range(0, valuesCount)
                    .map(idx => aggregationColumnNames[idx % aggregationsCount] + " (Value #" + Math.floor(idx / aggregationsCount) + ")");
                
                for (let i = 0; i < Math.min(valuesCount, namedColumns.length * aggregationsCount); i++) {
                    columnNames[i] = aggregationColumnNames[i % aggregationsCount] + " (" + namedColumns[Math.floor(i / aggregationsCount)] + ")";
                }
                return columnNames;
            } else {
                if (valuesCount > aggregationsCount) {
                    // looks like we have aggregation on more than one value - include value index 
                    return _.range(0, valuesCount)
                        .map(idx => aggregationColumnNames[idx % aggregationsCount] + " (Value #" + Math.floor(idx / aggregationsCount) + ")");
                } else {
                    return aggregationColumnNames;
                }
            }
        } else {
            const columnNames = _.range(0, valuesCount)
                .map(idx => "Value #" + idx);
            
            if (namedColumns) {
                for (let i = 0; i < Math.min(valuesCount, namedColumns.length); i++) {
                    columnNames[i] = namedColumns[i];
                }
            }
            
            return columnNames;
        }
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
            
            const columnNames = this.getValueColumnNames(valuesCount);
            
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
    }
    
    private editItem(item: Raven.Client.Documents.Session.TimeSeries.TimeSeriesEntry) {
        const editTimeSeriesDialog = new editTimeSeriesEntry(
            this.documentId(), 
            this.activeDatabase(), 
            this.timeSeriesName(), 
            this.getValueColumnNames(1024),
            item
        );
        app.showBootstrapDialog(editTimeSeriesDialog)
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
                const names = stats.TimeSeries.map(x => x.Name);
                this.timeSeriesNames(names);
                this.timeSeriesName(null);
                return { can: true };
            });
    }
    
    private activateById(docId: string, timeSeriesName: string) {
        return this.loadTimeSeries(docId)
            .then(stats => {
                const names = stats.TimeSeries.map(x => x.Name);
                
                if (_.includes(names, timeSeriesName)) {
                    this.timeSeriesName(timeSeriesName);
                    this.timeSeriesNames(names);
                    return { can: true };
                } else {
                    messagePublisher.reportWarning("Unable to find time series with name: " + timeSeriesName);
                    return { redirect: appUrl.forEditDoc(docId, this.activeDatabase()) }
                }
            });
    }
    
    loadTimeSeries(docId: string) {
        return new getTimeSeriesStatsCommand(docId, this.activeDatabase())
            .execute()
            .done(stats => {
                this.timeSeriesNames(stats.TimeSeries.map(x => x.Name));
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
        const createTimeSeriesDialog = new editTimeSeriesEntry(this.documentId(), this.activeDatabase(), tsNameToUse, []);
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
        if (_.includes(this.timeSeriesNames(), seriesName)) {
            this.changeCurrentSeries(seriesName);
        } else {
            // looks like user create new time series - need to reload data from server
            this.loadTimeSeries(this.documentId())
                .done(() => {
                    this.changeCurrentSeries(seriesName);
                });
        }
    }

    private initObservables() {
        this.isAggregation = ko.pureComputed(() => this.timeSeriesName() && this.timeSeriesName().includes("@"));
        
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
    }
}

export = editTimeSeries;
