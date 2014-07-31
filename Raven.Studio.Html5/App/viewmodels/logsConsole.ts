import changeSubscription = require('models/changeSubscription');
import viewModelBase = require('viewmodels/viewModelBase');
import shell = require("viewmodels/shell");
import changesApi = require("common/changesApi");
import appUrl = require("common/appUrl");

class logsConsole extends  viewModelBase {
    
    allLogsNotifications = ko.observableArray<logNotificationDto>([]);
    logChangesApi:changesApi;
    constructor() {
        super();

    }
    activate(args) {
        super.activate(args);
    }

    cleanupNotifications() {
        super.cleanupNotifications();
        if (!!this.logChangesApi) {
            this.logChangesApi.dispose();
        }
    }

    createNotifications(): Array<changeSubscription> {
        this.logChangesApi = new changesApi(this.activeDatabase(),0,true);
        return [
            this.logChangesApi.watchAdminLogs((e: logNotificationDto) => {
                
                if (this.allLogsNotifications().length == 1000) {
                    this.allLogsNotifications.shift();
                }
                this.allLogsNotifications.push(e);
                var objDiv = document.getElementById("logNotificationsContainer");
                objDiv.scrollTop = objDiv.scrollHeight;
            })
        ];
    }

    formatLogRecord(logRecord: logNotificationDto) {
        return 'Request #' + logRecord.RequestId.toString().paddingRight(' ', 4) + ' ' + logRecord.HttpMethod.paddingLeft(' ', 7) + ' - ' + logRecord.EllapsedMiliseconds.toString().paddingRight(' ', 5) + ' ms - ' + logRecord.TenantName.paddingLeft(' ', 10) + ' - ' + logRecord.ResponseStatusCode + ' - ' + logRecord.RequestUri;
    }

}

export = logsConsole;