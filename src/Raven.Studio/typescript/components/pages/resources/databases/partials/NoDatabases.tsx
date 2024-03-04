import React, { useState } from "react";
import { Button } from "reactstrap";
import { EmptySet } from "components/common/EmptySet";
import { useAccessManager } from "hooks/useAccessManager";
import { FlexGrow } from "components/common/FlexGrow";
import CreateDatabase, {
    CreateDatabaseMode,
} from "components/pages/resources/databases/partials/create/CreateDatabase";

export function NoDatabases() {
    const { isOperatorOrAbove } = useAccessManager();
    const [createDatabaseMode, setCreateDatabaseMode] = useState<CreateDatabaseMode>(null);

    return (
        <div className="content-margin">
            <div className="text-center">
                <EmptySet>No databases have been created</EmptySet>

                {isOperatorOrAbove() && (
                    <div className="d-flex gap-1">
                        <FlexGrow />
                        <Button outline color="primary" onClick={() => setCreateDatabaseMode("regular")}>
                            Create new database
                        </Button>
                        <div>or</div>
                        <Button outline color="primary" onClick={() => setCreateDatabaseMode("fromBackup")}>
                            Create one from backup
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
