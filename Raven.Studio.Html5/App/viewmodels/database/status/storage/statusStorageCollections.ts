import getSlowDocCountsCommand = require("commands/database/debug/getSlowDocCountsCommand");
import viewModelBase = require("viewmodels/viewModelBase");
import debugDocumentStats = require("models/database/debug/debugDocumentStats");
import genUtils = require("common/generalUtils");

class statusStorageCollections extends viewModelBase {
    data = ko.observable<debugDocumentStats>();
    canSearch = ko.observable(true);


    activate(args) {
        super.activate(args);

        this.activeDatabase.subscribe(() => this.resetView());
        return this.resetView();
    }

    resetView() {
        this.data(null);
        this.canSearch(true);
    }

    formatTimeSpan(input: string) {
        var timeParts = input.split(":");
        var miliPart;
        var sec = 0, milisec = 0;
        if (timeParts.length == 3) {
            miliPart = timeParts[2].split(".");
            sec = parseInt(miliPart[0]);
            var tmpMili;
            if (miliPart[1][0] == '0') {
                tmpMili = miliPart[1].substring(1, 3);
            } else {
                tmpMili = miliPart[1].substring(0, 3);
            }
            milisec = parseInt(tmpMili);
        }
        var hours = parseInt(timeParts[0]);
        var min = parseInt(timeParts[1]);

        var timeStr = "";
        if (hours > 0) {
            timeStr = hours + " Hours ";
        }
        if (min > 0) {
            timeStr += min + " Min ";
        }
        if (sec > 0) {
            timeStr += sec + " Sec ";
        }
        if ((timeStr == "") && (milisec > 0)) {
            timeStr = milisec + " Ms ";
        }
        return timeStr;
    }
    formatBytesToSize(bytes: number) {
        var sizes = ['Bytes', 'KB', 'MB', 'GB', 'TB'];
        if (bytes == 0) return 'n/a';
        var i = Math.floor(Math.log(bytes) / Math.log(1024));

        if (i < 0) {
            // number < 1
            return genUtils.formatAsCommaSeperatedString(bytes, 4) + ' Bytes';
        }

        var res = bytes / Math.pow(1024, i);
        var newRes = genUtils.formatAsCommaSeperatedString(res, 2);

        return newRes + ' ' + sizes[i];
    }



    fetchDocCounts(): JQueryPromise<debugDocumentStats> {
        var db = this.activeDatabase();
        if (db) {
            this.canSearch(false);
            return new getSlowDocCountsCommand(db)
                .execute()
                .done((results: debugDocumentStats) => {
                    this.data(results);
                })

                .always(() => this.canSearch(true));

        }

        return null;
    }
}

export = statusStorageCollections;