/// <reference path="../../../../../typings/tsd.d.ts" />

import registration = require("viewmodels/shell/registration");
import license = require("models/auth/licenseModel");
import moment = require("moment");

class licenseAgpl  {
    
    view = require("views/common/notificationCenter/customControlls/licenseAgpl.html");
    
    getView() {
        return this.view;
    }
    
    canUseUntil = ko.pureComputed(() => {
        const licenseStatus = license.licenseStatus();
        if (!licenseStatus) {
            return null;
        }

        const now = moment();
        const firstStart = moment(licenseStatus.FirstServerStartDate);
        const weekAfterFirstStart = firstStart.clone().add("1", "week");

        return now.isBefore(weekAfterFirstStart) ? weekAfterFirstStart.format("YYYY-MM-DD") : null; 
    });
    
    register() {
        registration.showRegistrationDialog(license.licenseStatus(), false, true);
    }
}

export = licenseAgpl;
