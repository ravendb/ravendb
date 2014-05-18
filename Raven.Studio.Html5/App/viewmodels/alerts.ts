import app = require("durandal/app");
import getAlertsCommand = require("commands/getAlertsCommand");
import viewModelBase = require("viewmodels/viewModelBase");
import getDatabaseStatsCommand = require("commands/getDatabaseStatsCommand");
import database = require("models/database");
import moment = require("moment");
import copyDocuments = require("viewmodels/copyDocuments");
import document = require("models/document");
import alert = require("models/alert");
import saveAlertsCommand = require("commands/saveAlertsCommand");

class alerts extends viewModelBase {

    alertDoc = ko.observable<alertContainerDto>();
    allAlerts = ko.observableArray<alert>();
    filterLevel = ko.observable("All");
    selectedAlert = ko.observable<alert>();
    unreadAlertCount: KnockoutComputed<number>;
    readAlertCount: KnockoutComputed<number>;
    now = ko.observable<Moment>();
    updateNowTimeoutHandle = 0;

    constructor() {
        super();

        this.unreadAlertCount = ko.computed(() => this.allAlerts().count(a => a.observed() === false));
        this.readAlertCount = ko.computed(() => this.allAlerts().count(a => a.observed() === true));
        this.updateCurrentNowTime();
        this.activeDatabase.subscribe(() => this.fetchAlerts());
    }

    activate(args) {
        super.activate(args);
        this.fetchAlerts();
    }

    deactivate() {
        clearTimeout(this.updateNowTimeoutHandle);
    }

    fetchAlerts(): JQueryPromise<alertContainerDto> {
        var db = this.activeDatabase();
        if (db) {
            return new getAlertsCommand(db)
                .execute()
                .done((result: alertContainerDto) => this.processAlertsResults(result))
        }

        return null;
    }

    processAlertsResults(result: alertContainerDto) {
        var now = moment();
        var alerts = result.Alerts.map(a => new alert(a));
        alerts.forEach(r => {
            r.createdAtHumanized = this.createHumanReadableTime(r.createdAt),
            r.isVisible = ko.computed(() => this.matchesFilter(r));
        });
        this.alertDoc(result);
        this.allAlerts(alerts);
    }

    matchesFilter(a: alert): boolean {
        if (this.filterLevel() === "All") {
            return true;
        }

        var unreadFilterWithUnreadAlert = this.filterLevel() === "Unread" && a.observed() === false;
        var readFilterWithReadAlert = this.filterLevel() === "Read" && a.observed() === true;
        return unreadFilterWithUnreadAlert || readFilterWithReadAlert;
    }

    createHumanReadableTime(time: string): KnockoutComputed<string> {
        if (time) {
            // Return a computed that returns a humanized string based off the current time, e.g. "7 minutes ago".
            // It's a computed so that it updates whenever we update this.now (scheduled to occur every minute.)
            return ko.computed(() => {
                var dateMoment = moment(time);
                var agoInMs = dateMoment.diff(this.now());
                return moment.duration(agoInMs).humanize(true) + dateMoment.format(" (MM/DD/YY, h:mma)");
            });
        }

        return ko.computed(() => time);
    }

    selectAlert(selection: alert) {
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
                var newSelectedRow = $("#alertsContainer table tbody tr:nth-child(" + (newSelectionIndex + 1) + ")");
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

    setFilterAll() {
        this.filterLevel("All");
    }

    setFilterUnread() {
        this.filterLevel("Unread");
    }

    setFilterRead() {
        this.filterLevel("Read");
    }

    updateCurrentNowTime() {
        this.now(moment());
        this.updateNowTimeoutHandle = setTimeout(() => this.updateCurrentNowTime(), 60000);
    }

    toggleSelectedReadState() {
        var alert = this.selectedAlert();
        if (alert) {
            alert.observed(!alert.observed());
        }
    }

    deleteSelectedAlert() {
        var alert = this.selectedAlert();
        if (alert) {
            this.allAlerts.remove(alert);
        }
    }

    deleteReadAlerts() {
        this.allAlerts.remove(a => a.observed());
    }

    deleteAllAlerts() {
        this.allAlerts.removeAll();
    }

    saveAlerts() {
        var alertDoc = this.alertDoc();
        var db = this.activeDatabase();
        if (alertDoc && db) {
            alertDoc.Alerts = this.allAlerts().map(a => a.toDto());
            new saveAlertsCommand(alertDoc, db).execute();
        }
    }
}

export = alerts;