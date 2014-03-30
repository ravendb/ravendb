import viewModelBase = require("viewmodels/viewModelBase");
import getReplicationStatsCommand = require("commands/getReplicationStatsCommand");
import moment = require("moment");

class replicationStats extends viewModelBase {

    replStatsDoc = ko.observable<replicationStatsDocumentDto>();
    hasNoReplStatsAvailable = ko.observable(false);
    now = ko.observable<Moment>();
    updateNowTimeoutHandle = 0;

    constructor() {
        super();

        this.updateCurrentNowTime();
    }

    activate(args) {
        super.activate(args);

        this.activeDatabase.subscribe(() => this.fetchReplStats());
        this.fetchReplStats();
    }

    fetchReplStats() {
        this.replStatsDoc(null);
        new getReplicationStatsCommand(this.activeDatabase())
            .execute()
            .fail(() => this.hasNoReplStatsAvailable(true)) // If replication is not setup, the fetch will fail with 404.
            .done((result: replicationStatsDocumentDto) => {
                this.hasNoReplStatsAvailable(result.Stats.length === 0);
                this.processResults(result);
            });
    }

    processResults(results: replicationStatsDocumentDto) {
        if (results) {
            results.Stats.forEach(s => {
                s['LastReplicatedLastModifiedHumanized'] = this.createHumanReadableTime(s.LastReplicatedLastModified);
                s['LastFailureTimestampHumanized'] = this.createHumanReadableTime(s.LastFailureTimestamp);
                s['LastHeartbeatReceivedHumanized'] = this.createHumanReadableTime(s.LastHeartbeatReceived);
                s['LastSuccessTimestampHumanized'] = this.createHumanReadableTime(s.LastSuccessTimestamp);
            });
        }

        this.replStatsDoc(results)
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

    updateCurrentNowTime() {
        this.now(moment());
        this.updateNowTimeoutHandle = setTimeout(() => this.updateCurrentNowTime(), 60000);
    }
}

export = replicationStats;