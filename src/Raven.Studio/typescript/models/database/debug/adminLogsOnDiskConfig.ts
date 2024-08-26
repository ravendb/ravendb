/// <reference path="../../../../typings/tsd.d.ts"/>
import genUtils = require("common/generalUtils");

class adminLogsOnDiskConfig {
    selectedLogMode = ko.observable<TODO>();

    fullPath = ko.observable<string>();
    retentionTime = ko.observable<string>();
    retentionSize = ko.observable<number>();
    compress: boolean;

    retentionTimeText: KnockoutComputed<string>;
    retentionSizeText: KnockoutComputed<string>;

    constructor(logsConfiguration: adminLogsConfiguration) {
        this.selectedLogMode(logsConfiguration.CurrentMode);
        this.fullPath(logsConfiguration.Path);
        
        this.initObservables();
    }
    
    private initObservables() {
        this.retentionTimeText = ko.pureComputed(() => {
            return "TODO retentionTimeText";
        });

        this.retentionSizeText = ko.pureComputed(() => {
          return "TODO retentionSizeText";
        });
    }
}

export = adminLogsOnDiskConfig;
