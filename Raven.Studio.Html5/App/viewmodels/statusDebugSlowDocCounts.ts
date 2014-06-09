import getStatusDebugSlowDocCountsCommand = require("commands/getStatusDebugSlowDocCountsCommand");
import appUrl = require("common/appUrl");
import database = require("models/database");
import viewModelBase = require("viewmodels/viewModelBase");
import debugDocumentStats = require("models/debugDocumentStats");
import genUtils = require("common/generalUtils");

class statusDebugSlowDocCounts extends viewModelBase {
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
    //formatAsCommaSeperatedString(input, digitsAfterDecimalPoint) {
    //    var parts = input.toString().split(".");
    //    parts[0] = parts[0].replace(/\B(?=(\d{3})+(?!\d))/g, ",");

    //    if (parts.length == 2 && parts[1].length > digitsAfterDecimalPoint) {
    //        parts[1] = parts[1].substring(0, digitsAfterDecimalPoint);
    //    }
    //    return parts.join(".");
    //}

    formatTimeSpan(input:string) {
        var timeParts = input.split(":");
        var miliPart ;
        var sec=0, milisec=0;
        if (timeParts.length == 3) {
            miliPart = timeParts[2].split(".");
            sec = parseInt(miliPart[0]);
            var tmpMili;
            if (miliPart[1][0] == '0') {
                tmpMili = miliPart[1].substring(1, 3);
            } else
            {
                tmpMili = miliPart[1].substring(0, 3); 
            }
            milisec = parseInt(tmpMili);
        }
        var hours = parseInt(timeParts[0]);
        var min = parseInt(timeParts[1]);

        var timeStr="";
        if (hours > 0) {
            timeStr = hours + " Hours ";
        }
        if (min > 0) {
            timeStr += min + " Min";
        }
        if (sec > 0) {
            timeStr += sec + " Sec";
        }
        if ((timeStr == "") && (milisec > 0))
        {
            timeStr = milisec + " Milisec ";
        }
        return timeStr;
    }
    formatBytesToSize(bytes: number){
        var sizes = ['Bytes', 'KB', 'MB', 'GB', 'TB'];
        if (bytes == 0) return 'n/a';
        var i = Math.floor(Math.log(bytes) / Math.log(1024));

        var res = bytes / Math.pow(1024, i);
        var newRes = genUtils.formatAsCommaSeperatedString(res, 2);   
        //var parts = res.toString().split(".");
        //parts[0] = parts[0].replace(/\B(?=(\d{3})+(?!\d))/g, ",");
        //if (parts.length == 2 && parts[1].length > 2) {
        //    parts[1] = parts[1].substring(0, 2);
        //}
        //var newRes = parts.join(".");
        return newRes + ' ' + sizes[i];
    }  
   
        //var i = -1;
        //var byteUnits = [' kB', ' MB', ' GB', ' TB', 'PB', 'EB', 'ZB', 'YB'];
        //do {
        //    fileSizeInBytes = fileSizeInBytes / 1024;
        //    i++;
        //} while (fileSizeInBytes > 1024);

        //return Math.max(fileSizeInBytes, 0.1).toFixed(1) + byteUnits[i];
   
    fetchDocCounts(): JQueryPromise<debugDocumentStats> {
        var db = this.activeDatabase();
        if (db) {
            this.canSearch(false);
            return new getStatusDebugSlowDocCountsCommand(db)
                .execute()
                .done((results: debugDocumentStats)=> {
                    this.data(results);
                   // formatBytesToSize(this.data.)
                })
                   
                .always(() => this.canSearch(true));

        }

        return null;
    }
}

export = statusDebugSlowDocCounts;