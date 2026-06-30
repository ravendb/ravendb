import React, { memo } from "react";
import { useAsync } from "react-async-hook";
import { useServices } from "hooks/useServices";
import { EmptySet } from "components/common/EmptySet";
import { RichAlert } from "components/common/RichAlert";
import SizeGetter from "components/common/SizeGetter";
import { IndexErrorsPanelTable } from "components/pages/database/indexes/errors/IndexErrorsPanelTable";
import useIndexErrorsTable from "components/pages/database/indexes/errors/hooks/useIndexErrorsTable";
import { indexErrorsUtils } from "components/pages/database/indexes/errors/IndexErrorsUtils";

const EMPTY_MAPPED_ERRORS: IndexErrorPerDocument[] = [];

interface DatabaseIndexErrorsProps {
    packageId: string;
    database: string;
    node: string;
}

// Reuses the live Index Errors rendering (IndexErrorsPanelTable + columns + mapItems) for a debug
// package: fetch the per-node errors from the analyzer endpoint, map them with the same util, and
// render the same table. disableLinks drops the edit-index / view-document hyperlinks, which point
// at the live server and are meaningless for a package snapshot.
export default memo(function DatabaseIndexErrors({ packageId, database, node }: DatabaseIndexErrorsProps) {
    const { manageServerService } = useServices();
    const { indexErrorsPanelTable } = useIndexErrorsTable();

    const errors = useAsync(async () => {
        if (!node) {
            return [] as IndexErrorPerDocument[];
        }
        const dto = await manageServerService.getDebugPackageDatabaseIndexErrors(packageId, node, database);
        return indexErrorsUtils.mapItems(dto);
    }, [packageId, node, database]);
    const mappedErrors = errors.result ?? EMPTY_MAPPED_ERRORS;
    const isEmpty = errors.status === "success" && mappedErrors.length === 0;

    return (
        <div className="database-index-errors">
            <div className="panel-bg-1 rounded">
                <div className="p-4">
                    <h3 className="m-0 mb-3">Index Errors</h3>
                    {errors.status === "error" ? (
                        <RichAlert variant="warning">
                            Could not load index errors for {database} on node {node}. The analysis report may have
                            expired on the server (re-upload the package to inspect), or this database&apos;s index data
                            was not captured.
                        </RichAlert>
                    ) : isEmpty ? (
                        <EmptySet compact className="justify-content-center">
                            No index errors for {database} on node {node}
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
            </div>
        </div>
    );
});
