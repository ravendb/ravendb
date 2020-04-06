/// <reference path="../../../../typings/tsd.d.ts"/>
import genUtils = require("common/generalUtils");

class adminLogsOnDiskConfig {
    possibleLogModes: Array<Sparrow.Logging.LogMode> = ["Operations", "Information"];
    
    currentServerLogMode = ko.observable<Sparrow.Logging.LogMode>();
    selectedLogMode = ko.observable<Sparrow.Logging.LogMode>();

    fullPath = ko.observable<string>();
    retentionTime = ko.observable<string>();
    retentionSize = ko.observable<number>();
    compress: boolean;

    retentionTimeText: KnockoutComputed<string>;
    retentionSizeText: KnockoutComputed<string>;

    constructor() {
        this.retentionTimeText = ko.pureComputed(() => {
            return this.retentionTime() === genUtils.timeSpanMaxValue ? "Unlimited" : genUtils.formatTimeSpan(this.retentionTime());
        });

        this.retentionSizeText = ko.pureComputed(() => {
            return this.retentionSize() ? genUtils.formatBytesToSize(this.retentionSize()) : "Unlimited";
        });
    }
}

export = adminLogsOnDiskConfig;
