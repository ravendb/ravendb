/// <reference path="../../../typings/tsd.d.ts"/>

import resource = require("models/resources/resource");
import license = require("models/auth/license");
import timeSeriesStatistics = require("models/timeSeries/timeSeriesStatistics");

class timeSeries extends resource {
    static readonly type = "timeSeries";
    static readonly qualifier = "ts";

    constructor(name: string, isAdminCurrentTenant: boolean = true, bundles: string[] = []) {
        super(name, isAdminCurrentTenant, bundles);
        if (!name) {
            debugger;
        }
        /* TODO
        this.isLicensed = ko.computed(() => {
            if (!!license.licenseStatus() && license.licenseStatus().IsCommercial) {
                var timeSeriesValue = license.licenseStatus().Attributes.timeSeries;
                return /^true$/i.test(timeSeriesValue);
            }
            return true;
        });*/
    }

    get qualifier() {
        return timeSeries.qualifier;
    }

    get urlPrefix() {
        return "ts";
    }

    get fullTypeName() {
        return "Time Series";
    }

    get type() {
        return timeSeries.type;
    }

    static getNameFromUrl(url: string) {
        var index = url.indexOf("timeseries/");
        return (index > 0) ? url.substring(index + 10) : "";
    }
}
export = timeSeries;
