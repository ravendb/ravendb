import React from "react";
import createDatabase from "viewmodels/resources/createDatabase";
import app from "durandal/app";
import { withPreventDefault } from "components/utils/common";
import { Button } from "reactstrap";
import { EmptySet } from "components/common/EmptySet";
import { useAccessManager } from "hooks/useAccessManager";
import { FlexGrow } from "components/common/FlexGrow";

export function NoDatabases() {
    const { isOperatorOrAbove } = useAccessManager();
    const newDatabase = () => {
        const createDbView = new createDatabase("newDatabase");
        app.showBootstrapDialog(createDbView);
    };

    const newDatabaseFromBackup = () => {
        const createDbView = new createDatabase("restore");
        app.showBootstrapDialog(createDbView);
    };

    return (
        <div className="content-margin">
            <div className="text-center">
                <EmptySet>No databases have been created</EmptySet>

                {isOperatorOrAbove() && (
                    <div className="d-flex gap-1">
                        <FlexGrow />
                        <Button outline color="primary" onClick={withPreventDefault(newDatabase)}>
                            Create new database
                        </Button>
                        <div>or</div>
                        <Button outline color="primary" onClick={withPreventDefault(newDatabaseFromBackup)}>
                            Create one from backup
                        </Button>
                        <FlexGrow />
                    </div>
                )}
            </div>
        </div>
    );
}
