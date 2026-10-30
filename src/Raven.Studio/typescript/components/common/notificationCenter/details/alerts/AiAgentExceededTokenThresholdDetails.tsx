import { ColumnDef } from "@tanstack/react-table";
import Alert from "react-bootstrap/Alert";
import alert from "common/notifications/models/alert";
import { useKnockoutValue } from "hooks/useKnockoutValue";
import { NotificationDetailsProps } from "../notificationDetailsProvider";
import { AlertDetailsModal } from "../NotificationDetailsModal";
import { NotificationDetailsTable } from "../NotificationDetailsTable";
import { NotificationDetailsList, NotificationDetailsListItem } from "../NotificationDetailsList";

type ToolCallDetails =
    Raven.Server.NotificationCenter.Notifications.Details.ExceededTokenThresholdDetails.ToolCallDetails;

const columns: ColumnDef<ToolCallDetails>[] = [
    {
        header: "Tool Name",
        accessorKey: "Name",
        meta: { relativeWidth: 20 },
    },
    {
        header: "Type",
        accessorKey: "Type",
        meta: { relativeWidth: 15 },
    },
    {
        header: "Arguments",
        accessorKey: "Arguments",
        meta: { relativeWidth: 25 },
    },
    {
        header: "Query",
        accessorKey: "Query",
        meta: { relativeWidth: 25 },
    },
    {
        header: "ID",
        accessorKey: "Id",
        meta: { relativeWidth: 15 },
    },
];

export default function AiAgentExceededTokenThresholdDetails({
    notification,
    actions,
}: NotificationDetailsProps<alert<"AiAgent_ExceededTokenThreshold">>) {
    const details = useKnockoutValue(notification.details);
    const toolCalls = details.ToolCalls ?? [];

    return (
        <AlertDetailsModal notification={notification} actions={actions} size="xxl">
            <NotificationDetailsList>
                <NotificationDetailsListItem label="Agent Name">{details.AgentName}</NotificationDetailsListItem>
                <NotificationDetailsListItem label="Conversation ID">
                    {details.ConversationId}
                </NotificationDetailsListItem>
                <NotificationDetailsListItem label="Token Count">
                    {details.TokenCount.toLocaleString()}
                </NotificationDetailsListItem>
                <NotificationDetailsListItem label="Token Threshold">
                    {details.TokenThreshold.toLocaleString()}
                </NotificationDetailsListItem>
            </NotificationDetailsList>
            {details.Recommendation && (
                <Alert variant="info" className="mb-0">
                    <strong>Recommendation:</strong>
                    <div>{details.Recommendation}</div>
                </Alert>
            )}
            {toolCalls.length > 0 && (
                <NotificationDetailsTable
                    notification={notification}
                    data={toolCalls}
                    columns={columns}
                    itemName="tool call"
                    itemNamePlural="tool calls"
                />
            )}
        </AlertDetailsModal>
    );
}
