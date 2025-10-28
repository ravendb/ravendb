import app = require("durandal/app");
import abstractNotification = require("common/notifications/models/abstractNotification");
import notificationCenter = require("common/notifications/notificationCenter");
import virtualGridController = require("widgets/virtualGrid/virtualGridController");
import textColumn = require("widgets/virtualGrid/columns/textColumn");
import alert = require("common/notifications/models/alert");
import columnPreviewPlugin = require("widgets/virtualGrid/columnPreviewPlugin");
import abstractAlertDetails = require("viewmodels/common/notificationCenter/detailViewer/alerts/abstractAlertDetails");
import ExceededTokenThresholdDetails = Raven.Server.NotificationCenter.Notifications.Details.ExceededTokenThresholdDetails;
import genUtils = require("common/generalUtils");

interface ToolCallItem {
    id: string;
    name: string;
    type: string;
    arguments: string;
}

class aiAgentExceededTokenThreshold extends abstractAlertDetails {
    view = require("views/common/notificationCenter/detailViewer/alerts/aiAgentExceededTokenThreshold.html");

    agentName: string;
    conversationId: string;
    tokenCount: number;
    tokenThreshold: number;
    recommendation: string;
    tableItems: ToolCallItem[] = [];
    private gridController = ko.observable<virtualGridController<ToolCallItem>>();
    private columnPreview = new columnPreviewPlugin<ToolCallItem>();

    constructor(alert: alert, notificationCenter: notificationCenter) {
        super(alert, notificationCenter);

        const details = this.alert.details() as ExceededTokenThresholdDetails;
        
        this.agentName = details.AgentName;
        this.conversationId = details.ConversationId;
        this.tokenCount = details.TokenCount;
        this.tokenThreshold = details.TokenThreshold;
        this.recommendation = details.Recommendation;
        
        this.tableItems = (details.ToolCalls || []).map(tc => ({
            id: tc.Id,
            name: tc.Name,
            type: tc.Type,
            arguments: tc.Arguments
        }));
    }
    
    compositionComplete() {
        super.compositionComplete();

        const grid = this.gridController();
        grid.headerVisible(true);

        grid.init(() => this.fetcher(), () => {
            const nameColumn = new textColumn<ToolCallItem>(grid, x => x.name, "Tool Name", "20%", {
                sortable: x => x.name
            });
            const typeColumn = new textColumn<ToolCallItem>(grid, x => x.type, "Type", "15%", {
                sortable: x => x.type
            });
            const argumentsColumn = new textColumn<ToolCallItem>(grid, x => x.arguments, "Arguments", "50%");
            const idColumn = new textColumn<ToolCallItem>(grid, x => x.id, "ID", "15%");

            return [nameColumn, typeColumn, argumentsColumn, idColumn];
        });

        this.columnPreview.install(".aiAgentExceededTokenThresholdDetails", ".js-ai-agent-token-threshold-tooltip",
            (details: ToolCallItem,
             column: textColumn<ToolCallItem>,
             e: JQuery.TriggeredEvent, onValue: (context: any, valueToCopy?: string) => void) => {
                const value = column.getCellValue(details);
                if (value) {
                    onValue(genUtils.escapeHtml(value), value);
                }
            });
    }
    
    private fetcher(): JQueryPromise<pagedResult<ToolCallItem>> {
        return $.Deferred<pagedResult<ToolCallItem>>()
            .resolve({
                items: this.tableItems,
                totalResultCount: this.tableItems.length
            });
    }
    
    static supportsDetailsFor(notification: abstractNotification) {
        return (notification instanceof alert) && notification.alertType() === "AiAgent_ExceededTokenThreshold";
    }

    static showDetailsFor(alert: alert, center: notificationCenter) {
        return app.showBootstrapDialog(new aiAgentExceededTokenThreshold(alert, center));
    }
}

export = aiAgentExceededTokenThreshold;
