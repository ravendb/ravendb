/// <reference path="../../../typings/tsd.d.ts"/>

import resource = require("models/resources/resource");
import license = require("models/auth/license");
import timeSeriesStatistics = require("models/timeSeries/timeSeriesStatistics");

class timeSeries extends resource {
    statistics = ko.observable<timeSeriesStatistics>();
    static type = "timeSeries";
    iconName = "fa fa-clock-o";

    constructor(name: string, isAdminCurrentTenant: boolean = true, isDisabled: boolean = false) {
        super(name, TenantType.TimeSeries, isAdminCurrentTenant);
        if (!name) {
            debugger;
        }
        this.fullTypeName = "Time Series";
        this.disabled(isDisabled);
        this.itemCountText = ko.computed(() => !!this.statistics() ? this.statistics().keysCountText() : "");
        this.isLicensed = ko.computed(() => {
            if (!!license.licenseStatus() && license.licenseStatus().IsCommercial) {
                var timeSeriesValue = license.licenseStatus().Attributes.timeSeries;
                return /^true$/i.test(timeSeriesValue);
            }
            return true;
        });
    }

    activate() {
        this.isLoaded(true);
        ko.postbox.publish("ActivateTimeSeries", this);
    }

    saveStatistics(dto: timeSeriesStatisticsDto) {
        if (!this.statistics()) {
            this.statistics(new timeSeriesStatistics());
        }

        this.statistics().fromDto(dto);
    }

    static getNameFromUrl(url: string) {
        var index = url.indexOf("timeseries/");
        return (index > 0) ? url.substring(index + 10) : "";
    }
}
export = timeSeries;
