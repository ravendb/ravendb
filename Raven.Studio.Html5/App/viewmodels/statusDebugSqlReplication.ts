import getStatusDebugSqlReplicationCommand = require("commands/getStatusDebugSqlReplicationCommand");
import appUrl = require("common/appUrl");
import database = require("models/database");
import viewModelBase = require("viewmodels/viewModelBase");
import sqlReplicationStats = require("common/sqlReplicationStats");


class statusDebugSqlReplication extends viewModelBase {
    data = ko.observable<sqlReplicationStats[]>();
    columnWidths: Array<KnockoutObservable<number>>;
    constructor() {
        super();
    }
    
    activate(args) {
        var widthUnit = 8;
        this.columnWidths = [
            ko.observable<number>(12),  
            ko.observable<number>(widthUnit),
            ko.observable<number>(widthUnit),
            ko.observable<number>(widthUnit),
            ko.observable<number>(widthUnit),
            ko.observable<number>(10),
            ko.observable<number>(3* widthUnit),
            ko.observable<number>(widthUnit),//Name
            ko.observable<number>(widthUnit),//Counter
            ko.observable<number>(widthUnit),//Max
            ko.observable<number>(widthUnit),//Min
            ko.observable<number>(widthUnit),//Stdev
            ko.observable<number>(4*widthUnit),//Percentiles
            ko.observable<number>(widthUnit*2),
            ko.observable<number>(widthUnit*2),
            ko.observable<number>(widthUnit*2),
            ko.observable<number>(widthUnit*2),
            ko.observable<number>(widthUnit*2),
            ko.observable<number>(widthUnit*2)
        ];
        super.activate(args);

        this.activeDatabase.subscribe(() => this.fetchSqlReplicationStats());
        this.registerColumnResizing();
        return this.fetchSqlReplicationStats();
    }

    deactivate() {
        this.unregisterColumnResizing();
    }

    fetchSqlReplicationStats(): JQueryPromise<sqlReplicationStatsDto[]> {
        var db = this.activeDatabase();
        if (db) {
            return new getStatusDebugSqlReplicationCommand(db)
                .execute()
                .done((results: sqlReplicationStatsDto[]) => {
                    this.data(results.map((x: sqlReplicationStatsDto)=> new sqlReplicationStats(x)));
            });
        }

        return null;
    }

    registerColumnResizing() {
        var resizingColumn = false;
        var startX = 0;
        var startingWidth = 0;
        var columnIndex = 0;

        $(document).on("mousedown.logTableColumnResize", ".column-handle", (e: any) => {
            columnIndex = parseInt($(e.currentTarget).attr("column"));
            startingWidth = this.columnWidths[columnIndex]();
            startX = e.pageX;
            resizingColumn = true;
        });

        $(document).on("mouseup.logTableColumnResize", "", (e: any) => {
            resizingColumn = false;
        });

        $(document).on("mousemove.logTableColumnResize", "", (e: any) => {
            if (resizingColumn) {
                var W = window;
                var elem = e.toElement;
                var parent = $(elem).parent()[0];
                var parentFontSize = parseInt(W.getComputedStyle(parent,null).fontSize, 10),
                    elemFontSize = parseInt(W.getComputedStyle(elem, null).fontSize, 10);
                var pxInEms = Math.floor((elemFontSize / parentFontSize) * 100) / 10;
                
                var targetColumnSize = startingWidth + (e.pageX - startX) / pxInEms;
                this.columnWidths[columnIndex](targetColumnSize);

                // Stop propagation of the event so the text selection doesn't fire up
                if (e.stopPropagation) e.stopPropagation();
                if (e.preventDefault) e.preventDefault();
                e.cancelBubble = true;
                e.returnValue = false;

                return false;
            }
        });
    }

    unregisterColumnResizing() {
        $(document).off("mousedown.logTableColumnResize");
        $(document).off("mouseup.logTableColumnResize");
        $(document).off("mousemove.logTableColumnResize");
    }
}

export = statusDebugSqlReplication;