import React, { useEffect, useState } from "react";
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
import { Icon } from "components/common/Icon";
import { Alert, Button, Collapse } from "reactstrap";
import useBoolean from "hooks/useBoolean";
import VirtualGrid from "components/common/VirtualGrid";
import virtualGridController from "widgets/virtualGrid/virtualGridController";
import virtualColumn from "widgets/virtualGrid/columns/virtualColumn";
import textColumn from "widgets/virtualGrid/columns/textColumn";
import useConfirm from "components/common/ConfirmDialog";
import { todo } from "common/developmentHelper";

interface IndexErrorsPanelProps {
    hasErrors: boolean;
}

interface SomeData {
    show: any;
    name: string;
    documentId: string;
    date: string;
    action: string;
    error: string;
}

export function IndexErrorsPanel(props: IndexErrorsPanelProps) {
    const { hasErrors } = props;
    const { value: panelCollapsed, toggle: togglePanelCollapsed } = useBoolean(true);
    const confirm = useConfirm();
    const onDeleteUser = async () => {
        const isConfirmed = await confirm({
            title: (
                <span>
                    Clear errors for <Icon icon="node" color="node" margin="m-0" /> <strong>A</strong>{" "}
                    <Icon icon="shard" color="shard" margin="m-0" /> <strong>$0</strong>
                </span>
            ),
            message: (
                <div>
                    Errors will be cleared for <strong>all indexes in the selected location</strong>.
                    <Alert color="info" className="mt-3">
                        <Icon icon="info" />
                        While the current indexing errors will be cleared, an index with an <em>Error state</em> will
                        not be set back to <em>Normal</em> state.
                    </Alert>
                </div>
            ),
            actionColor: "warning",
            icon: "trash",
            confirmText: "Clear errors",
        });
    };
    const [counter, setCounter] = useState<number>(0);
    const [gridController, setGridController] = useState<virtualGridController<SomeData>>();
    useEffect(() => {
        if (!gridController) {
            return;
        }

        gridController.headerVisible(true);
        gridController.init(
            () => fetcher(counter),
            () => columnsProvider(gridController)
        );
    }, [counter, gridController]);

    useEffect(() => {
        gridController?.reset();
    }, [counter, gridController]);

    todo("Feature", "Damian", "Add logic");
    todo("Feature", "Damian", "Fix Virtual Grid");
    todo("Feature", "Damian", "Connect to studio");
    todo("Feature", "Damian", "Remove old code");

    return (
        <RichPanel className="flex-row with-status">
            {hasErrors ? (
                <RichPanelStatus color="danger">Errors</RichPanelStatus>
            ) : (
                <RichPanelStatus color="success">OK</RichPanelStatus>
            )}

            <div className="flex-grow-1">
                <RichPanelHeader>
                    <RichPanelInfo>
                        <RichPanelName className="d-flex gap-3">
                            <span>
                                <Icon icon="node" color="node" margin="m-0" /> A
                            </span>
                            <span>
                                <Icon icon="shard" color="shard" margin="m-0" /> $0
                            </span>
                        </RichPanelName>
                    </RichPanelInfo>
                    {hasErrors && (
                        <RichPanelActions>
                            <Button color="warning" onClick={onDeleteUser}>
                                <Icon icon="trash" />
                                Clear errors
                            </Button>
                        </RichPanelActions>
                    )}
                </RichPanelHeader>
                {hasErrors && (
                    <>
                        <RichPanelDetails className="pb-0">
                            <RichPanelDetailItem>
                                <Button
                                    onClick={togglePanelCollapsed}
                                    title={panelCollapsed ? "Expand errors details" : "Collapse errors details"}
                                    className="btn-toggle-panel rounded-pill"
                                >
                                    <Icon icon={panelCollapsed ? "unfold" : "fold"} margin="m-0" />
                                </Button>
                            </RichPanelDetailItem>
                            <RichPanelDetailItem>
                                <span className="text-danger">
                                    <Icon icon="warning" />
                                    Total count
                                </span>
                                <div className="value">500 errors</div>
                            </RichPanelDetailItem>
                            <RichPanelDetailItem>
                                <span>
                                    <Icon icon="clock" />
                                    Most recent
                                </span>
                                <div className="value">2023 November 30th, 3:05 PM</div>
                            </RichPanelDetailItem>
                        </RichPanelDetails>
                        <Collapse isOpen={!panelCollapsed}>
                            <RichPanelDetails>
                                <div className="flex-grow-1 position-relative">
                                    <div style={{ position: "relative", height: "300px" }}>
                                        <VirtualGrid<SomeData> setGridController={setGridController} />
                                    </div>
                                </div>
                            </RichPanelDetails>
                        </Collapse>
                    </>
                )}
            </div>
        </RichPanel>
    );
}

const fetcher = (counter: number) => {
    return $.Deferred<pagedResult<SomeData>>().resolve({
        items: new Array(10).fill(null).map((_, id) => ({
            show: `Show`,
            name: `Errored`,
            documentId: `orders/${counter * id}-A`,
            date: `2024 April 23rd, 5:30 PM`,
            action: `Map`,
            error: `Failed to execute mapping function`,
        })),
        totalResultCount: 10,
    });
};

const columnsProvider = (gridController: virtualGridController<SomeData>): virtualColumn[] => {
    return [
        new textColumn<SomeData>(gridController, (x) => x.show, "Show", "10%"),
        new textColumn<SomeData>(gridController, (x) => x.name, "Index name", "20%", {
            sortable: "string",
        }),
        new textColumn<SomeData>(gridController, (x) => x.documentId, "Document ID", "20%", {
            sortable: "string",
        }),
        new textColumn<SomeData>(gridController, (x) => x.date, "Date", "20%", {
            sortable: "string",
        }),
        new textColumn<SomeData>(gridController, (x) => x.action, "Action", "10%", {
            sortable: "string",
        }),
        new textColumn<SomeData>(gridController, (x) => x.error, "Error", "20%", {
            sortable: "string",
        }),
    ];
};
