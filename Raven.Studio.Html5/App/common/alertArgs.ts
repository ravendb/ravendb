import alertType = require("common/alertType");

class alertArgs {
    public id: string;

    constructor(public type: alertType, public title: string, public details?: string) {
        this.id = "alert_" + new Date().getMilliseconds().toString() + "_" + title.length.toString() + "_" + (details ? details.length.toString() : '0');
    }
}

export = alertArgs;