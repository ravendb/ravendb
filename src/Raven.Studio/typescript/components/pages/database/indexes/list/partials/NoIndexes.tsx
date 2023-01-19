import { useAppUrls } from "hooks/useAppUrls";
import { useAccessManager } from "hooks/useAccessManager";
import { EmptySet } from "components/common/EmptySet";
import { Button } from "reactstrap";
import React from "react";
import database from "models/resources/database";

interface NoIndexesProps {
    database: database;
}

export function NoIndexes(props: NoIndexesProps) {
    const { database } = props;
    const { forCurrentDatabase } = useAppUrls();
    const newIndexUrl = forCurrentDatabase.newIndex();
    const { canReadWriteDatabase } = useAccessManager();

    return (
        <div className="text-center">
            <EmptySet>No indexes have been created for this database.</EmptySet>

            {canReadWriteDatabase(database) && (
                <Button outline color="primary" href={newIndexUrl}>
                    Create new index
                </Button>
            )}
        </div>
    );
}
