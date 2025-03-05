import React, { useCallback, useMemo, useState } from "react";
import Badge from "react-bootstrap/Badge";
import NavItem from "react-bootstrap/NavItem";
import NavLink from "react-bootstrap/NavLink";
import Nav from "react-bootstrap/Nav";
import classNames from "classnames";
import "./ClusterDebugEntries.scss";
import { useAppSelector } from "components/store";
import { clusterSelectors } from "components/common/shell/clusterSlice";
import { useServices } from "hooks/useServices";
import { useAsync } from "react-async-hook";
import VirtualTable from "components/common/virtualTable/VirtualTable";
import { getCoreRowModel, useReactTable } from "@tanstack/react-table";
import { useClusterDebugColumns } from "components/pages/resources/manageServer/advanced/clusterDebug/partials/useClusterDebugColumns";
import Code from "components/common/Code";
import useDialog from "components/common/Dialog";
import useConfirm from "components/common/ConfirmDialog";

interface ClusterDebugEntriesProps {
    availableWidth: number;
}

//TODO: add pagining

export function ClusterDebugEntries(props: ClusterDebugEntriesProps) {
    const { availableWidth } = props;
    const localNode = useAppSelector(clusterSelectors.localNode);
    const allNodes = useAppSelector(clusterSelectors.allNodes);

    const dialog = useDialog();
    const confirm = useConfirm();
    const { manageServerService } = useServices();

    const [activeTab, setActiveTab] = useState<string>(localNode.nodeTag);

    const { loading, result } = useAsync(
        (nodeTag: string) => manageServerService.getClusterLog(nodeTag, undefined, 1001),
        [activeTab]
    );

    const showInlinePreview = useCallback(
        async (logIndex: number) => {
            const entry = await manageServerService.getClusterLogEntry(activeTab, logIndex);
            const jsonString = JSON.stringify(entry, null, 4);
            await dialog({
                title: "Cluster Log Entry",
                message: <Code elementToCopy={jsonString} code={jsonString} language="json" />,
                modalSize: "lg",
            });
        },
        [manageServerService, activeTab, dialog]
    );

    const deleteEntry = useCallback(
        async (logIndex: number) => {
            const isConfirmed = await confirm({
                title: "Are you sure?",
                confirmText: "I understand the risk, delete",
                message: (
                    <div>
                        Do you want to delete log item with index <code>{logIndex}</code> from cluster log?
                    </div>
                ),
            });

            if (isConfirmed) {
                await manageServerService.removeClusterEntryLog(activeTab, logIndex);
            }
        },
        [manageServerService, activeTab, confirm]
    );

    const { columns } = useClusterDebugColumns(availableWidth, result?.Log.CommitIndex, showInlinePreview, deleteEntry);

    const data = useMemo(() => {
        return result?.Log.Logs ?? [];
    }, [result]);

    const table = useReactTable({
        data,
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
                {allNodes.map((node) => (
                    <NavItem key={node.nodeTag}>
                        <NavLink
                            className={classNames({ active: activeTab === node.nodeTag }, "no-decor")}
                            onClick={() => setActiveTab(node.nodeTag)}
                        >
                            <div className="d-flex gap-1 align-items-center">
                                <span>
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

            <VirtualTable table={table} heightInPx={500} isLoading={loading} />
        </div>
    );
}

export default ClusterDebugEntries;
