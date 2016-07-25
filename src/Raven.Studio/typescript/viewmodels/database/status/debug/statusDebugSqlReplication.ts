import getStatusDebugSqlReplicationCommand = require("commands/database/debug/getStatusDebugSqlReplicationCommand");
import viewModelBase = require("viewmodels/viewModelBase");
import sqlReplicationStats = require("common/sqlReplicationStats");


class statusDebugSqlReplication extends viewModelBase {
    data = ko.observable<sqlReplicationStats[]>();
    columnWidths: Array<KnockoutObservable<number>>;
    constructor() {
        super();
    }
    
    activate(args: any) {
        var widthUnit = 8;
        this.columnWidths = [
            ko.observable<number>(widthUnit),  
            ko.observable<number>(12),
            ko.observable<number>(widthUnit),
            ko.observable<number>(widthUnit),
            ko.observable<number>(widthUnit),
            ko.observable<number>(widthUnit),
            ko.observable<number>(10),
            ko.observable<number>(6),//Table
            ko.observable<number>(18),//Name
            ko.observable<number>(6),//Counter
            ko.observable<number>(6),//Max
            ko.observable<number>(6),//Min
            ko.observable<number>(6),//Stdev
            ko.observable<number>(6),//Percentiles
            ko.observable<number>(4*6),
            ko.observable<number>(6),
            ko.observable<number>(6),
            ko.observable<number>(6),
            ko.observable<number>(6),
            ko.observable<number>(6),
            ko.observable<number>(6)
        ];
        super.activate(args);
        this.updateHelpLink('JHZ574');
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
                var elemFontSize = parseInt(window.getComputedStyle(e.toElement, null).fontSize, 10);
                
                var targetColumnSize = startingWidth + (e.pageX - startX) / elemFontSize;
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
