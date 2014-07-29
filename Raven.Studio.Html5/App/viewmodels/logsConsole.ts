import changeSubscription = require('models/changeSubscription');
import viewModelBase = require('viewmodels/viewModelBase');
import shell = require("viewmodels/shell");


class logsConsole extends  viewModelBase {
    
    allLogsNotifications = ko.observableArray<logNotificationDto>([]);

    constructor() {
        super();

    }

    createNotifications(): Array<changeSubscription> {
        return [
            shell.currentResourceChangesApi().watchAdminLogs((e: logNotificationDto) => {
                
                if (this.allLogsNotifications().length == 50) {
                    this.allLogsNotifications.pop();
                }
                this.allLogsNotifications.push(e);
                
            })
        ];
    }

}

export = logsConsole;