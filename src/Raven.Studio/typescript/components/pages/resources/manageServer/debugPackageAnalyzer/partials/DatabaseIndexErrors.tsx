import React, { useState } from "react";
import { useAsync } from "react-async-hook";
import { useServices } from "hooks/useServices";
import { EmptySet } from "components/common/EmptySet";
import { RichAlert } from "components/common/RichAlert";
import Select, { SelectOption } from "components/common/select/Select";
import SizeGetter from "components/common/SizeGetter";
import { IndexErrorsPanelTable } from "components/pages/database/indexes/errors/IndexErrorsPanelTable";
import useIndexErrorsTable from "components/pages/database/indexes/errors/hooks/useIndexErrorsTable";
import { indexErrorsUtils } from "components/pages/database/indexes/errors/IndexErrorsUtils";

interface DatabaseIndexErrorsProps {
    packageId: string;
    database: string;
    nodes: string[];
}

// Reuses the live Index Errors rendering (IndexErrorsPanelTable + columns + mapItems) for a debug
// package: fetch the per-node errors from the analyzer endpoint, map them with the same util, and
// render the same table. disableLinks drops the edit-index / view-document hyperlinks, which point
// at the live server and are meaningless for a package snapshot.
export default function DatabaseIndexErrors({ packageId, database, nodes }: DatabaseIndexErrorsProps) {
    const { manageServerService } = useServices();
    const { indexErrorsPanelTable } = useIndexErrorsTable();
    const [selectedNode, setSelectedNode] = useState<string>(nodes[0] ?? null);

    const errors = useAsync(async () => {
        if (!selectedNode) {
            return [] as IndexErrorPerDocument[];
        }
        const dto = await manageServerService.getDebugPackageDatabaseIndexErrors(packageId, selectedNode, database);
        return indexErrorsUtils.mapItems(dto);
    }, [packageId, selectedNode, database]);

    const nodeOptions: SelectOption<string>[] = nodes.map((tag) => ({ value: tag, label: `Node ${tag}` }));
    const mappedErrors = errors.result ?? [];
    const isEmpty = errors.status === "success" && mappedErrors.length === 0;

    return (
        <div className="database-index-errors">
            <div className="hstack gap-3 align-items-center mb-3 flex-wrap">
                <h3 className="m-0">Index Errors</h3>
                {nodes.length > 1 && (
                    <div className="node-select">
                        <Select
                            options={nodeOptions}
                            value={nodeOptions.find((o) => o.value === selectedNode)}
                            onChange={(option) => option && setSelectedNode(option.value)}
                            isSearchable={false}
                            isRoundedPill
                        />
                    </div>
                )}
            </div>
            {errors.status === "error" ? (
                <RichAlert variant="warning">
                    Could not load index errors for {database} on node {selectedNode}. The analysis report may have
                    expired on the server (re-upload the package to inspect), or this database&apos;s index data was not
                    captured.
                </RichAlert>
            ) : isEmpty ? (
                <EmptySet compact>
                    No index errors for {database} on node {selectedNode}
                </EmptySet>
            ) : (
                <SizeGetter
                    render={(size) => (
                        <IndexErrorsPanelTable
                            status={errors.status}
                            isLoading={errors.loading}
                            refresh={errors.execute}
                            indexErrors={mappedErrors}
                            width={size.width}
                            table={indexErrorsPanelTable}
                            disableLinks
                        />
                    )}
                />
            )}
        </div>
    );
}
