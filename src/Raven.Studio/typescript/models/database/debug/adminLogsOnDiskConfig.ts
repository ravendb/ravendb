/// <reference path="../../../../typings/tsd.d.ts"/>
import genUtils = require("common/generalUtils");

class adminLogsOnDiskConfig {
    possibleLogModes: Array<Sparrow.Logging.LogMode> = ["Operations", "Information"];
    selectedLogMode = ko.observable<Sparrow.Logging.LogMode>();

    fullPath = ko.observable<string>();
    retentionTime = ko.observable<string>();
    retentionSize = ko.observable<number>();
    compress: boolean;

    retentionTimeText: KnockoutComputed<string>;
    retentionSizeText: KnockoutComputed<string>;

    constructor(logsConfiguration: adminLogsConfiguration) {
        this.selectedLogMode(logsConfiguration.CurrentMode);
        this.fullPath(logsConfiguration.Path);
        this.retentionTime(logsConfiguration.RetentionTime);
        this.retentionSize(logsConfiguration.RetentionSize);
        this.compress = logsConfiguration.Compress;
        
        this.initObservables();
    }
    
    private initObservables() {
        this.retentionTimeText = ko.pureComputed(() => {
            return this.retentionTime() === genUtils.timeSpanMaxValue ? "Unlimited" : genUtils.formatTimeSpan(this.retentionTime());
        });

        this.retentionSizeText = ko.pureComputed(() => {
            return this.retentionSize() ? genUtils.formatBytesToSize(this.retentionSize()) : "Unlimited";
        });
    }
}

export = adminLogsOnDiskConfig;
