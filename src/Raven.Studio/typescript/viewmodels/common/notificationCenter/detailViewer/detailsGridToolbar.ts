import abstractNotification = require("common/notifications/models/abstractNotification");
import virtualGridController = require("widgets/virtualGrid/virtualGridController");
import fileDownloader = require("common/fileDownloader");
import pluralizeHelpers = require("common/helpers/text/pluralizeHelpers");
import moment = require("moment");
import virtualGridCsv = require("widgets/virtualGrid/virtualGridCsv");

class detailsGridToolbar<T extends object> {
    readonly summary: KnockoutComputed<string>;
    readonly canExport: KnockoutComputed<boolean>;

    constructor(
        private readonly gridController: KnockoutObservable<virtualGridController<T>>,
        private readonly notification: abstractNotification,
        itemName: string,
        itemsName: string
    ) {
        const totalCount = ko.pureComputed(() => this.gridController()?.selection()?.totalCount);

        this.summary = ko.pureComputed(() =>
            totalCount() == null ? "" : pluralizeHelpers.pluralize(totalCount(), itemName, itemsName)
        );
        this.canExport = ko.pureComputed(() => totalCount() > 0);
        this.exportToCsv = this.exportToCsv.bind(this);
    }

    exportToCsv() {
        const grid = this.gridController();
        fileDownloader.downloadAsCsv(virtualGridCsv.gridToCsv(grid.getColumns(), grid.getItems()), this.fileName());
    }

    private fileName() {
        const parts = [this.notification.databaseName, this.notification.title(), moment().format("YYYY-MM-DD-HHmm")];
        return parts.filter((part) => part).map(detailsGridToolbar.toFileNamePart).join("-") + ".csv";
    }

    private static toFileNamePart(value: string) {
        return value
            .toLowerCase()
            .replace(/[^a-z0-9]+/g, "-")
            .replace(/^-|-$/g, "");
    }
}

export = detailsGridToolbar;
