import React, { useCallback } from "react";
import createDatabase from "viewmodels/resources/createDatabase";
import app from "durandal/app";
import { withPreventDefault } from "../../../utils/common";
import { Button } from "reactstrap";
import { EmptySet } from "components/common/EmptySet";
import { useAccessManager } from "hooks/useAccessManager";

export function NoDatabases() {
    const { isOperatorOrAbove } = useAccessManager();
    const newDatabase = useCallback(() => {
        const createDbView = new createDatabase("newDatabase");
        app.showBootstrapDialog(createDbView);
    }, []);

    //TODO: from backup

    return (
        <div
            data-bind="if: databases().sortedDatabases().length === 0, visible: databases().sortedDatabases().length === 0"
            className="content-margin"
        >
            <div className="text-center">
                <EmptySet>No databases have been created</EmptySet>

                {isOperatorOrAbove() && (
                    <div>
                        <Button outline color="primary" onClick={withPreventDefault(newDatabase)}>
                            Create new database
                        </Button>
                        <br />
                        {/* TODO or <a href="#" data-bind="click: newDatabaseFromBackup">create one from backup </a> */}
                    </div>
                )}
            </div>
        </div>
    );
}
