import React from "react";
import {
    RichPanel,
    RichPanelActions,
    RichPanelDetailItem,
    RichPanelDetails,
    RichPanelHeader,
    RichPanelInfo,
    RichPanelName,
    RichPanelStatus,
} from "components/common/RichPanel";
import { Button, Collapse } from "reactstrap";
import { Icon } from "components/common/Icon";
import Code from "components/common/Code";

interface EntryData {
    command: string;
    status: "Committed" | "Appended" | "Failed";
    index: number;
    uniqueRequestId: string;
}

interface ClusterDebugEntryProps {
    entry: EntryData;
    panelCollapsed: boolean;
    togglePanelCollapsed: () => void;
}

export const entriesData: EntryData[] = [
    {
        command: "UpdatePeriodicBackupStatusCommand",
        status: "Committed",
        index: 1,
        uniqueRequestId: "4ca44cf2-b8c5-4152-8045-ae93dfdb8b0a",
    },
    {
        command: "EditTimeSeriesConfigurationCommand",
        status: "Appended",
        index: 2,
        uniqueRequestId: "f3cea530-1e59-4277-83bf-dec95cb318e3/time-series",
    },
    {
        command: "PutIndexesCommand",
        status: "Failed",
        index: 3,
        uniqueRequestId: "58c933ba-f075-420b-8b50-b1b4c68a21cd",
    },
];

const getStatusColor = (status: EntryData["status"]): string => {
    switch (status) {
        case "Committed":
            return "success";
        case "Appended":
            return "info";
        case "Failed":
            return "danger";
        default:
            return "secondary";
    }
};

const formattedJson = `{
    "Type": "RachisEntry",
    "Index": 21,
    "Term": 4,
    "Entry": {
        "Type": "Noop for A in term 4",
        "Command": "noop",
        "UniqueRequestId": "e4fc1f35-29e6-4df3-8881-f7df886ba99a"
    },
    "Flags": "Noop"
}`;

export function ClusterDebugEntry(props: ClusterDebugEntryProps) {
    const { entry, panelCollapsed, togglePanelCollapsed } = props;
    return (
        <RichPanel className="flex-row with-status">
            <RichPanelStatus color={getStatusColor(entry.status)}>{entry.status}</RichPanelStatus>
            <div className="flex-grow-1">
                <RichPanelHeader className="flex-horizontal p-2">
                    <RichPanelInfo>
                        <RichPanelName>{entry.command}</RichPanelName>
                    </RichPanelInfo>
                    <RichPanelActions>
                        <Button color="danger">
                            <Icon icon="trash" margin="m-0" />
                        </Button>
                    </RichPanelActions>
                </RichPanelHeader>
                <RichPanelDetails>
                    <RichPanelDetailItem>
                        <Button
                            color="secondary"
                            title={panelCollapsed ? "Expand distribution details" : "Collapse distribution details"}
                            onClick={togglePanelCollapsed}
                            className="ms-1 btn-toggle-panel rounded-pill"
                        >
                            <Icon icon={panelCollapsed ? "unfold" : "fold"} margin="m-0" />
                        </Button>
                    </RichPanelDetailItem>
                    <RichPanelDetailItem label="Index">{entry.index}</RichPanelDetailItem>
                    <RichPanelDetailItem label="Unique request id">{entry.uniqueRequestId}</RichPanelDetailItem>
                </RichPanelDetails>
                <div className="px-4 pb-2">
                    <Collapse isOpen={!panelCollapsed}>
                        <Code code={formattedJson} language="json" />
                    </Collapse>
                </div>
            </div>
        </RichPanel>
    );
}
