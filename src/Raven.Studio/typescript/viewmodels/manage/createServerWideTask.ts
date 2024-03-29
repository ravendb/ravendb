import eventsCollector = require("common/eventsCollector");
import appUrl = require("common/appUrl");
import router = require("plugins/router");
import dialogViewModelBase = require("viewmodels/dialogViewModelBase");
import licenseModel from "models/auth/licenseModel"; 

class createServerWideTask extends dialogViewModelBase {

    hasServerWideBackups = licenseModel.getStatusValue("HasServerWideBackups");
    hasServerWideExternalReplications = licenseModel.getStatusValue("HasServerWideExternalReplications");
    
    view = require("views/manage/createServerWideTask.html");

    newServerWideReplicationTask() {
        eventsCollector.default.reportEvent("serverWideExternalReplication", "new");
        const url = appUrl.forEditServerWideExternalReplication();
        router.navigate(url);
        this.close();
    }

    newServerWideBackupTask() {
        eventsCollector.default.reportEvent("serverWidePeriodicBackup", "new");
        const url = appUrl.forEditServerWideBackup();
        router.navigate(url);
        this.close();
    }
}

export = createServerWideTask;
