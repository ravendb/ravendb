import changeSubscription = require('models/changeSubscription');
import viewModelBase = require('viewmodels/viewModelBase');
import shell = require("viewmodels/shell");
import changesApi = require("common/changesApi");
import appUrl = require("common/appUrl");
import httpTraceClient = require("common/httpTraceClient");

class logsConsole extends  viewModelBase {
    
    allLogsNotifications = ko.observableArray<logNotificationDto>([]);
    loghttpTraceClient: httpTraceClient;
    constructor() {
        super();

    }

    canActivate(args) {
        super.canActivate(args);
        var canReadLogs = $.Deferred();        
        var tracedDB = (!!args && !!args.database ? (args.database !== "<system>" && this.activeDatabase().name == args.database? this.activeDatabase():null) : null);
        this.loghttpTraceClient = new httpTraceClient(tracedDB);
        this.loghttpTraceClient.connectToChangesApiTask
            .done(() => canReadLogs.resolve({ can: true }))
            .fail((e) => {
                if (!!e && !!e.status && e.status == 401) {
                    canReadLogs.resolve({ redirect: appUrl.forLogs(this.activeDatabase()) });
                } else {
                    canReadLogs.resolve({ can: true });
                }
        });

        return canReadLogs;
    }
    activate(args) {
        super.activate(args);
    }

    cleanupNotifications() {
        super.cleanupNotifications();
        if (!!this.loghttpTraceClient) {
            this.loghttpTraceClient.dispose();
        }
    }

    createNotifications(): Array<changeSubscription> {
        return [
            this.loghttpTraceClient.watchAdminLogs((e: logNotificationDto) => {
                
                if (this.allLogsNotifications().length == 1000) {
                    this.allLogsNotifications.shift();
                }
                this.allLogsNotifications.push(e);
                var objDiv = document.getElementById("logNotificationsContainer");
                var selection;

                if (window.getSelection) {
                    selection = window.getSelection().toString();
                } else if (document.selection && document.selection.type != "Control") {
                    selection = document.selection.createRange().text;
                }
                if (selection.length == 0)
                objDiv.scrollTop = objDiv.scrollHeight*1.1 ;
            })
        ];
    }

    formatLogRecord(logRecord: logNotificationDto) {
        return 'Request #' + logRecord.RequestId.toString().paddingRight(' ', 4) + ' ' + logRecord.HttpMethod.paddingLeft(' ', 7) + ' - ' + logRecord.EllapsedMiliseconds.toString().paddingRight(' ', 5) + ' ms - ' + logRecord.TenantName.paddingLeft(' ', 10) + ' - ' + logRecord.ResponseStatusCode + ' - ' + logRecord.RequestUri;
    }

}

export = logsConsole;