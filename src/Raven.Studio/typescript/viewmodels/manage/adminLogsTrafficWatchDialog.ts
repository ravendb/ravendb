import dialogViewModelBase = require("viewmodels/dialogViewModelBase");
import trafficWatchConfiguration = require("models/resources/trafficWatchConfiguration");
import dialog = require("plugins/dialog");
import awesomeMultiselect = require("common/awesomeMultiselect");
import databasesManager = require("common/shell/databasesManager");
import TrafficWatchChangeType = Raven.Client.Documents.Changes.TrafficWatchChangeType;
import licenseModel from "models/auth/licenseModel";

class adminLogsTrafficWatchDialog extends dialogViewModelBase {
    
    view = require("views/manage/adminLogsTrafficWatchDialog.html");

    private readonly model: trafficWatchConfiguration;

    usingHttps = location.protocol === "https:";

    canPersist = !licenseModel.cloudLicense();

    private allDatabaseNames = ko.observableArray<string>();
    private static allHttpMethods = ["GET", "POST", "PUT", "DELETE", "HEAD"];
    private static allChangeTypes: TrafficWatchChangeType[] = ["BulkDocs", "Counters", "Documents", "Hilo", "Index", "MultiGet", "Operations", "Queries", "Streams", "Subscriptions", "TimeSeries"];
    private static allStatusCodes: number[] = [
        101,
        200, 201, 202, 203, 204,
        301, 302, 304, 307, 308, 
        400, 401, 403, 404, 405, 408, 409, 415, 429,
        500, 501, 502, 503, 504, 505
    ];
    
    constructor(model: trafficWatchConfiguration) {
        super();
        
        this.model = model;
        this.allDatabaseNames(databasesManager.default.databases().map(x => x.name));
    }
    
    attached() {
        super.attached();
        
        awesomeMultiselect.build($("#trafficStatusCodes"), opts => {
            opts.includeSelectAllOption = true;
        });
        
        awesomeMultiselect.build($("#trafficHttpMethods"), opts => {
            opts.includeSelectAllOption = true;
        });

        awesomeMultiselect.build($("#trafficDatabaseNames"), opts => {
            opts.includeSelectAllOption = true;
        });

        awesomeMultiselect.build($("#trafficChangeTypes"), opts => {
            opts.includeSelectAllOption = true;
        });
    }
    
    save() {
        if (this.isValid(this.model.validationGroup)) {
            dialog.close(this, this.model);
        }
    }
}

export = adminLogsTrafficWatchDialog;
