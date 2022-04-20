import React, { useCallback } from "react";
import createDatabase from "viewmodels/resources/createDatabase";
import app from "durandal/app";
import { withPreventDefault } from "../../../utils/common";

export function NoDatabases() {
    const newDatabase = useCallback(() => {
        const createDbView = new createDatabase("newDatabase");
        app.showBootstrapDialog(createDbView);
    }, []);

    return (
        <div
            data-bind="if: databases().sortedDatabases().length === 0, visible: databases().sortedDatabases().length === 0"
            className="content-margin"
        >
            <div className="row">
                <div className="col-sm-8 col-sm-offset-2 col-lg-6 col-lg-offset-3">
                    <i className="icon-xl icon-empty-set text-muted" />
                    <h2 className="text-center text-muted">No databases have been created</h2>
                    <p className="lead text-center text-muted" data-bind="visible: accessManager.canCreateNewDatabase">
                        Go ahead and{" "}
                        <a href="#" onClick={withPreventDefault(newDatabase)}>
                            &nbsp;create one now
                        </a>
                        <br />
                        {/* TODO or <a href="#" data-bind="click: newDatabaseFromBackup">create one from backup </a> */}
                    </p>
                    <p className="lead text-center text-muted" />
                </div>
            </div>
        </div>
    );
}
