import React, { useState } from "react";
import { Button } from "reactstrap";
import { EmptySet } from "components/common/EmptySet";
import { FlexGrow } from "components/common/FlexGrow";
import CreateDatabase, {
    CreateDatabaseMode,
} from "components/pages/resources/databases/partials/create/CreateDatabase";
import { accessManagerSelectors } from "components/common/shell/accessManagerSliceSelectors";
import { useAppSelector } from "components/store";

export function NoDatabases() {
    const isOperatorOrAbove = useAppSelector(accessManagerSelectors.isOperatorOrAbove);
    const [createDatabaseMode, setCreateDatabaseMode] = useState<CreateDatabaseMode>(null);

    return (
        <div className="content-margin">
            <div className="text-center">
                <EmptySet>No databases have been created</EmptySet>

                {isOperatorOrAbove && (
                    <div className="d-flex gap-1">
                        <FlexGrow />
                        <Button outline color="primary" onClick={() => setCreateDatabaseMode("regular")}>
                            Create new database
                        </Button>
                        <div>or</div>
                        <Button outline color="primary" onClick={() => setCreateDatabaseMode("fromBackup")}>
                            Restore one from backup
                        </Button>
                        <FlexGrow />

                        {createDatabaseMode && (
                            <CreateDatabase
                                closeModal={() => setCreateDatabaseMode(null)}
                                initialMode={createDatabaseMode}
                            />
                        )}
                    </div>
                )}
            </div>
        </div>
    );
}
