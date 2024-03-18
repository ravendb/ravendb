import { useAppUrls } from "hooks/useAppUrls";
import { useAccessManager } from "hooks/useAccessManager";
import { EmptySet } from "components/common/EmptySet";
import { Button } from "reactstrap";
import React from "react";

export function NoIndexes() {
    const { forCurrentDatabase } = useAppUrls();
    const newIndexUrl = forCurrentDatabase.newIndex();
    const { canReadWriteDatabase } = useAccessManager();

    return (
        <div className="text-center">
            <EmptySet>No indexes have been created for this database.</EmptySet>

            {canReadWriteDatabase() && (
                <Button outline color="primary" href={newIndexUrl}>
                    Create new index
                </Button>
            )}
        </div>
    );
}
