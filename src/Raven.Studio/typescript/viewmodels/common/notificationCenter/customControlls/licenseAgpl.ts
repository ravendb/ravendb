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

        const now = moment.utc();
        const firstStart = moment.utc(licenseStatus.FirstServerStartDate);
        const weekAfterFirstStart = firstStart.clone().add("1", "week");
        const dateFormat = "YYYY-MM-DD HH:mm:ss";

        return now.isBefore(weekAfterFirstStart)
            ? `<strong>${weekAfterFirstStart.format(dateFormat)} UTC
                    <i class="icon-info text-info"
                        title="The Studio will be available for use until ${weekAfterFirstStart.format(dateFormat)} UTC, which is ${weekAfterFirstStart.local().format(dateFormat)} your local time.">
                    </i>
                </strong>`
            : null;
    });
    
    register() {
        registration.showRegistrationDialog(license.licenseStatus(), false, true);
    }
}

export = licenseAgpl;
