import viewModelBase = require("viewmodels/viewModelBase");
import getDatabaseStatsCommand = require("commands/getDatabaseStatsCommand");
import moment = require("moment");

class indexErrors extends viewModelBase {

    allIndexErrors = ko.observableArray<serverErrorDto>();
    hasFetchedErrors = ko.observable(false);
    selectedIndexError = ko.observable<serverErrorDto>();
    now = ko.observable<Moment>();
    updateNowTimeoutHandle = 0;

    constructor() {
        super();

        this.updateCurrentNowTime();
    }

    modelPolling() {
        return this.fetchIndexErrors();
    }

    deactivate() {
        clearTimeout(this.updateNowTimeoutHandle);
    }

    fetchIndexErrors(): JQueryPromise<databaseStatisticsDto> {
        var db = this.activeDatabase();
        if (db) {
            // Index errors are actually the .ServerErrors returned from the database statistics query.
            return new getDatabaseStatsCommand(db)
                .execute()
                .done((stats: databaseStatisticsDto) => {
                    stats.Errors.forEach(e => e['TimestampHumanized'] = this.createHumanReadableTime(e.Timestamp));
                    this.allIndexErrors(stats.Errors);
                    this.hasFetchedErrors(true);
                });
        }

        return null;
    }

    tableKeyDown() {
    }

    selectIndexError(indexError: serverErrorDto) {
        this.selectedIndexError(indexError);
    }

    createHumanReadableTime(time: string): KnockoutComputed<string> {
        if (time) {
            // Return a computed that returns a humanized string based off the current time, e.g. "7 minutes ago".
            // It's a computed so that it updates whenever we update this.now field.
            return ko.computed(() => {
                var dateMoment = moment(time);
                var agoInMs = dateMoment.diff(this.now());
                return moment.duration(agoInMs).humanize(true) + dateMoment.format(" (MM/DD/YY, h:mma)");
            });
        }

        return ko.computed(() => time);
    }

    updateCurrentNowTime() {
        this.now(moment());
        this.updateNowTimeoutHandle = setTimeout(() => this.updateCurrentNowTime(), 60000);
    }
}

export = indexErrors; 