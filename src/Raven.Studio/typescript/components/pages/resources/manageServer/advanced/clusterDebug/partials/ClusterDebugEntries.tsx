import React, { useCallback, useState } from "react";
import Badge from "react-bootstrap/Badge";
import NavItem from "react-bootstrap/NavItem";
import NavLink from "react-bootstrap/NavLink";
import Nav from "react-bootstrap/Nav";
import classNames from "classnames";
import "./ClusterDebugEntries.scss";
import { useAppSelector } from "components/store";
import { clusterSelectors } from "components/common/shell/clusterSlice";
import { useServices } from "hooks/useServices";
import VirtualTable from "components/common/virtualTable/VirtualTable";
import { getCoreRowModel, useReactTable } from "@tanstack/react-table";
import { useClusterDebugColumns } from "components/pages/resources/manageServer/advanced/clusterDebug/hooks/useClusterDebugColumns";
import Code from "components/common/Code";
import useDialog from "components/common/Dialog";
import useConfirm from "components/common/ConfirmDialog";
import { Icon } from "components/common/Icon";
import { ClusterDebugNodeInfo } from "components/pages/resources/manageServer/advanced/clusterDebug/partials/common";
import { nodeAwareLoadableData } from "hooks/useClusterWideAsync";
import RichAlert from "components/common/RichAlert";
import { useClusterDebugFetcher } from "../hooks/useClusterDebugFetcher";

interface ClusterDebugEntriesProps {
    availableWidth: number;
    nodes: nodeAwareLoadableData<ClusterDebugNodeInfo>[];
}

export function ClusterDebugEntries(props: ClusterDebugEntriesProps) {
    const { availableWidth, nodes } = props;
    const localNode = useAppSelector(clusterSelectors.localNode);

    const dialog = useDialog();
    const confirm = useConfirm();
    const { manageServerService } = useServices();

    const [activeTab, setActiveTab] = useState<string>(localNode.nodeTag);

    const showInlinePreview = useCallback(
        async (logIndex: number) => {
            const entry = await manageServerService.getClusterLogEntry(activeTab, logIndex);
            const jsonString = JSON.stringify(entry, null, 4);
            await dialog({
                title: "Cluster Log Entry",
                message: <Code elementToCopy={jsonString} code={jsonString} language="json" />,
                modalSize: "lg",
                container: document.getElementById("cluster-debug"),
            });
        },
        [manageServerService, activeTab, dialog]
    );

    const deleteEntry = useCallback(
        async (logIndex: number) => {
            const isConfirmed = await confirm({
                title: "Delete log entry?",
                icon: "trash",
                confirmText: "I understand the risk, delete",
                message: (
                    <div>
                        <p>
                            You are about to delete log entry with index <code>{logIndex}</code>.
                        </p>
                        <RichAlert variant="warning" icon="warning">
                            Deleting a log entry from the Raft command log can lead to data inconsistencies and cluster
                            instability. If all nodes are connected and there are no network errors, this Raft command
                            will be deleted from ALL nodes in the cluster. Proceed with caution.
                        </RichAlert>
                    </div>
                ),
                container: document.getElementById("cluster-debug"),
            });

            if (isConfirmed) {
                await manageServerService.removeClusterEntryLog(activeTab, logIndex);
            }
        },
        [manageServerService, activeTab, confirm]
    );

    const { dataArray, commitIndex, componentProps } = useClusterDebugFetcher({
        nodeTag: activeTab,
        dependencies: [activeTab],
    });

    const { columns } = useClusterDebugColumns(availableWidth, commitIndex, showInlinePreview, deleteEntry);

    const table = useReactTable({
        data: dataArray,
        defaultColumn: {
            enableColumnFilter: false,
            enableSorting: false,
        },
        columns,
        getCoreRowModel: getCoreRowModel(),
    });

    return (
        <div className="cluster-debug-entries">
            <Nav variant="tabs">
                {nodes.map((node) => (
                    <NavItem key={node.nodeTag}>
                        <NavLink
                            className={classNames({ active: activeTab === node.nodeTag }, "no-decor")}
                            onClick={() => setActiveTab(node.nodeTag)}
                        >
                            <div className="d-flex gap-1 align-items-center">
                                <span>
                                    <Icon
                                        icon={node.data?.role === "Leader" ? "node-leader" : "cluster-member"}
                                        color="node"
                                    />
                                    <span className="text-nowrap">Node {node.nodeTag}</span>
                                </span>
                                {node.nodeTag === localNode.nodeTag && (
                                    <Badge bg="node" pill>
                                        Current
                                    </Badge>
                                )}
                            </div>
                        </NavLink>
                    </NavItem>
                ))}
            </Nav>

            <VirtualTable table={table} heightInPx={500} {...componentProps} />
        </div>
    );
}

export default ClusterDebugEntries;
