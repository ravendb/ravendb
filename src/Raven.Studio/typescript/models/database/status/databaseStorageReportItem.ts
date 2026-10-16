/// <reference path="../../../../typings/tsd.d.ts"/>

import storageReportItem = require("models/database/status/storageReportItem");
import appUrl = require("common/appUrl");
import DatabaseUtils = require("components/utils/DatabaseUtils");
import StudioDatabaseState = Raven.Server.Web.System.Processors.Studio.StudioDatabasesHandlerForGetDatabasesState.StudioDatabaseState;

class databaseStorageReportItem extends storageReportItem {

    readonly isDisabled: boolean;

    constructor(state: StudioDatabaseState, isDisabled: boolean) {
        const isSizeKnown = state.TotalSize != null;
        const dataSize = state.TotalSize?.SizeInBytes ?? 0;
        const tempBuffersSize = state.TempBuffersSize?.SizeInBytes ?? 0;
        const type = DatabaseUtils.default.isSharded(state.Name) ? "shard" : "database";

        super(DatabaseUtils.default.formatName(state.Name), type, true, dataSize + tempBuffersSize);

        this.isDisabled = isDisabled;
        this.link = isDisabled ? null : appUrl.forStatusStorageReport(state.Name);

        if (isSizeKnown) {
            this.dataSize = dataSize;
            this.tempBuffersSize = tempBuffersSize;
        } else {
            this.customSizeProvider = () => "n/a";
        }
    }

    formatPercentage(parentSize: number) {
        return this.dataSize == null ? "n/a" : super.formatPercentage(parentSize);
    }
}

export = databaseStorageReportItem;
