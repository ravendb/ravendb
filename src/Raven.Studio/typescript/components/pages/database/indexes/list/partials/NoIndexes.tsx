import { useAppUrls } from "hooks/useAppUrls";
import { EmptySet } from "components/common/EmptySet";
import React from "react";
import { accessManagerSelectors } from "components/common/shell/accessManagerSliceSelectors";
import { useAppSelector } from "components/store";

export function NoIndexes() {
    const hasDatabaseWriteAccess = useAppSelector(accessManagerSelectors.getHasDatabaseWriteAccess)();
    const { forCurrentDatabase } = useAppUrls();

    return (
        <div className="text-center">
            <EmptySet>No indexes have been created for this database.</EmptySet>

            {hasDatabaseWriteAccess && (
                <div className="vstack align-items-center gap-2 mt-2">
                    <a href={forCurrentDatabase.newIndex()} className="btn btn-outline-primary">
                        Create new index
                    </a>
                    <a href={forCurrentDatabase.indexes(null, null, true)()} className="btn btn-outline-info">
                        Import indexes
                    </a>
                </div>
            )}
        </div>
    );
}
