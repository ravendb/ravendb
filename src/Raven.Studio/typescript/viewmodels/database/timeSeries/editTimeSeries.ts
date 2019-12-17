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

class editTimeSeries extends viewModelBase {
    static timeSeriesFormat = "YYYY-MM-DD HH:mm:ss.SSS";
    
    documentId = ko.observable<string>();
    timeSeriesName = ko.observable<string>();
    timeSeriesNames = ko.observableArray<string>([]);
    
    urlForDocument: KnockoutComputed<string>;

    private gridController = ko.observable<virtualGridController<Raven.Client.Documents.Session.TimeSeriesValue>>();
    private columnPreview = new columnPreviewPlugin<Raven.Client.Documents.Session.TimeSeriesValue>();
    
    constructor() {
        super();
        
        this.bindToCurrentInstance("changeCurrentSeries", "createTimeSeries");
        
        this.initObservables();
    }
    
    canActivate(args: any): JQueryPromise<canActivateResultDto> {
        return $.when<any>(super.canActivate(args))
            .then(() => {
                if (!args.name) {
                    return this.activateByCreateNew(args.docId);
                } else {
                    return this.activateById(args.docId, args.name);    
                }
            });
    }
    
    compositionComplete() {
        super.compositionComplete();
        
        if (!this.timeSeriesName()) {
            this.createTimeSeries(true);    
        }
        
        const formatTimeSeriesDate = (input: string) => {
            const dateToFormat = moment.utc(input);
            return dateToFormat.local().format(editTimeSeries.timeSeriesFormat);
        };
        
        const grid = this.gridController();
        grid.headerVisible(true);

        const editColumn = new actionColumn<Raven.Client.Documents.Session.TimeSeriesValue>(
            grid, item => this.editItem(item), "Edit", `<i class="icon-edit"></i>`, "70px",
            {
                title: () => 'Edit item'
            });
        
        grid.init((s, t) => this.fetchSeries(s, t), () =>
            [
                new checkedColumn(true),
                editColumn,
                new textColumn<Raven.Client.Documents.Session.TimeSeriesValue>(grid, x => formatTimeSeriesDate(x.Timestamp), "Date", "20%"),
                new textColumn<Raven.Client.Documents.Session.TimeSeriesValue>(grid, x => x.Tag || 'N/A', "Tag", "20%"),
                new textColumn<Raven.Client.Documents.Session.TimeSeriesValue>(grid, x => x.Values.join(", "), "Values", "40%")
            ]
        );

        this.columnPreview.install("virtual-grid", ".js-time-series-tooltip",
            (item: Raven.Client.Documents.Session.TimeSeriesValue, column: textColumn<Raven.Client.Documents.Session.TimeSeriesValue>,
             e: JQueryEventObject, onValue: (context: any, valueToCopy?: string) => void) => {
                const value = column.getCellValue && column.getCellValue(item);
                if (column.header === "Edit") {
                    return null;
                } else if (column.header === "Date") {
                    onValue(moment.utc(item.Timestamp), item.Timestamp);
                } else if (!_.isUndefined(value)) {
                    onValue(generalUtils.escapeHtml(value), value);
                }
            });
    }
    
    private editItem(item: Raven.Client.Documents.Session.TimeSeriesValue) {
        const editTimeSeriesDialog = new editTimeSeriesEntry(this.documentId(), this.activeDatabase(), this.timeSeriesName(), item);
        app.showBootstrapDialog(editTimeSeriesDialog)
            .done((seriesName) => {
                if (seriesName) {
                    this.refresh();    
                }
            });
    }

    private fetchSeries(skip: number, take: number): JQueryPromise<pagedResult<Raven.Client.Documents.Session.TimeSeriesValue>> {
        const fetchTask = $.Deferred<pagedResult<Raven.Client.Documents.Session.TimeSeriesValue>>();
        const timeSeriesName = this.timeSeriesName();

        if (timeSeriesName) {
            new getTimeSeriesCommand(this.documentId(), timeSeriesName, this.activeDatabase(), skip, take)
                .execute()
                .done(result => {
                    const seriesValues = result.Values[timeSeriesName];
                    const values = seriesValues && seriesValues.length > 0 ? seriesValues[0].Values : [];

                    fetchTask.resolve({
                        items: values,
                        totalResultCount: values.length
                    })
                });    
        } else {
            fetchTask.resolve({
                items: [],
                totalResultCount: 0
            })
        }
        
        return fetchTask;
    }

    private refresh() {
        this.gridController().reset(false);
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
    }
    
    changeCurrentSeries(name: string) {
        this.timeSeriesName(name);
        
        this.refresh();
    }
    
    createTimeSeries(createNew: boolean) {
        const tsNameToUse = createNew ? null : this.timeSeriesName();
        const createTimeSeriesDialog = new editTimeSeriesEntry(this.documentId(), this.activeDatabase(), tsNameToUse);
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
                    this.refresh();
                });
        }
    }

    private initObservables() {
        this.urlForDocument = ko.pureComputed(() => {
            return appUrl.forEditDoc(this.documentId(), this.activeDatabase());
        })
    }
}

export = editTimeSeries;
