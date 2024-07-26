import { useAppUrls } from "hooks/useAppUrls";
import { EmptySet } from "components/common/EmptySet";
import { Button } from "reactstrap";
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
                <Button outline color="primary" href={forCurrentDatabase.newIndex()}>
                    Create new index
                </Button>
            )}
        </div>
    );
}
