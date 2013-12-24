import app = require("durandal/app");
import getAlertsCommand = require("commands/getAlertsCommand");
import alert = require("models/alert");
import activeDbViewModelBase = require("viewmodels/activeDbViewModelBase");
import getDatabaseStatsCommand = require("commands/getDatabaseStatsCommand");
import database = require("models/database");
import moment = require("moment");
import copyDocuments = require("viewmodels/copyDocuments");
import document = require("models/document");

class alerts extends activeDbViewModelBase {

    allAlerts = ko.observableArray<alert>();
    filterLevel = ko.observable("All");
    selectedAlert = ko.observable<alert>();

    constructor() {
        super();
    }

    activate(args) {
        super.activate(args);
        this.fetchAlerts();
    }

    fetchAlerts(): JQueryPromise<alert[]> {
        var db = this.activeDatabase();
        if (db) {
            return new getAlertsCommand(db)
                .execute()
                .done((results: alert[]) => this.processAlertsResults(results))
                .fail(() => this.processAlertsResults([])); // When no alerts present, Raven shoots us back a 404. :-(
        }

        return null;
    }

    processAlertsResults(results: alert[]) {
        var now = moment();
        results.forEach(r => {
            r['createdAtText'] = this.createHumanReadableTime(r.createdAt, now);
            //r['isVisible'] = ko.computed(() => this.matchesFilterAndSearch(r));
        });
        this.allAlerts(results.reverse());
    }

    createHumanReadableTime(time: string, now: Moment) {
        if (time) {
            var dateMoment = moment(time);
            var agoInMs = dateMoment.diff(now);
            return moment.duration(agoInMs).humanize(true) + dateMoment.format(" (MM/DD/YY, h:mma)");
        }

        return time;
    }

    selectLog(selection: alert) {
        this.selectedAlert(selection);
    }

    tableKeyDown(sender: any, e: KeyboardEvent) {
        var isKeyUp = e.keyCode === 38;
        var isKeyDown = e.keyCode === 40;
        if (isKeyUp || isKeyDown) {
            e.preventDefault();

            var oldSelection = this.selectedAlert();
            if (oldSelection) {
                var oldSelectionIndex = this.allAlerts.indexOf(oldSelection);
                var newSelectionIndex = oldSelectionIndex;
                if (isKeyUp && oldSelectionIndex > 0) {
                    newSelectionIndex--;
                } else if (isKeyDown && oldSelectionIndex < this.allAlerts().length - 1) {
                    newSelectionIndex++;
                }

                this.selectedAlert(this.allAlerts()[newSelectionIndex]);
                var newSelectedRow = $("#logsContainer table tbody tr:nth-child(" + (newSelectionIndex + 1) + ")");
                if (newSelectedRow) {
                    this.ensureRowVisible(newSelectedRow);
                }
            }
        }
    }

    ensureRowVisible(row: JQuery) {
        var table = $("#alertTableContainer");
        var scrollTop = table.scrollTop();
        var scrollBottom = scrollTop + table.height();
        var scrollHeight = scrollBottom - scrollTop;

        var rowPosition = row.position();
        var rowTop = rowPosition.top;
        var rowBottom = rowTop + row.height();

        if (rowTop < 0) {
            table.scrollTop(scrollTop + rowTop);
        } else if (rowBottom > scrollHeight) {
            table.scrollTop(scrollTop + (rowBottom - scrollHeight));
        }
    }
}

export = alerts;