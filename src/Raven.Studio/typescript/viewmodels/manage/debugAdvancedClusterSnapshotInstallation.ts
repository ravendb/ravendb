import dialogViewModelBase from "viewmodels/dialogViewModelBase";
import columnPreviewPlugin from "widgets/virtualGrid/columnPreviewPlugin";
import virtualGridController from "widgets/virtualGrid/virtualGridController";
import textColumn from "widgets/virtualGrid/columns/textColumn";
import generalUtils = require("common/generalUtils");
import moment = require("moment");
import virtualColumn from "widgets/virtualGrid/columns/virtualColumn";

class debugAdvancedClusterSnapshotInstallation extends dialogViewModelBase {
    
    view = require("views/manage/debugAdvancedClusterSnapshotInstallation.html");

    tableItems: Raven.Server.Rachis.RachisDebugMessage[] = [];
    private gridController = ko.observable<virtualGridController<Raven.Server.Rachis.RachisDebugMessage>>();
    private columnPreview = new columnPreviewPlugin<Raven.Server.Rachis.RachisDebugMessage>();

    constructor(messages: Raven.Server.Rachis.RachisDebugMessage[]) {
        super();
        
        this.tableItems = messages;
    }

    compositionComplete() {
        super.compositionComplete();

        const grid = this.gridController();
        grid.headerVisible(true);
        grid.setDefaultSortBy(0, "asc");

        grid.init(() => this.fetcher(), () => {
            return [
                new textColumn<Raven.Server.Rachis.RachisDebugMessage>(grid, x => generalUtils.formatDurationByDate(moment.utc(x.At)), "Time", "20%", {
                    sortable: x => x.At
                }),
                new textColumn<Raven.Server.Rachis.RachisDebugMessage>(grid, x => x.Message, "Message", "75%", {
                    sortable: "string"
                }),
            ];
        });

        this.columnPreview.install(".clusterSnapshotInstallationDetails", ".js-cluster-snapshot-installation-details-tooltip",
            (details: Raven.Server.Rachis.RachisDebugMessage, column: virtualColumn, e: JQuery.TriggeredEvent, onValue: (context: any, valueToCopy: string) => void) => {
                if (column instanceof textColumn) {
                    const value = column.getCellValue(details);
                    if (column.header === "Time") {
                        onValue(moment.utc(details.At), details.At);
                    } else if (!_.isUndefined(value)) {
                        onValue(generalUtils.escapeHtml(value), value);
                    }
                }
            });
    }

    private fetcher(): JQueryPromise<pagedResult<Raven.Server.Rachis.RachisDebugMessage>> {
        return $.Deferred<pagedResult<Raven.Server.Rachis.RachisDebugMessage>>()
            .resolve({
                items: this.tableItems,
                totalResultCount: this.tableItems.length
            });
    }


}

export = debugAdvancedClusterSnapshotInstallation;
