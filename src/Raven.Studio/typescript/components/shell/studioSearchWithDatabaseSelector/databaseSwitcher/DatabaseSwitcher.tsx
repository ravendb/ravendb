import "./DatabaseSwitcher.scss";
import React, { useEffect, useState } from "react";
import databasesManager from "common/shell/databasesManager";
import { Icon } from "components/common/Icon";
import { databaseSelectors } from "components/common/shell/databaseSliceSelectors";
import { useAppSelector } from "components/store";
import { UncontrolledDropdown, DropdownToggle, DropdownMenu, DropdownItem } from "reactstrap";

export default function DatabaseSwitcher() {
    const allDatabaseNames = useAppSelector(databaseSelectors.allDatabaseNames);
    const activeDatabaseName = useAppSelector(databaseSelectors.activeDatabaseName);

    // Switching database firstly sets the active database name to null
    // To avoid short blinking after the change to null, we wait 500ms
    const [activeDatabaseNameWithDelay, setActiveDatabaseNameWithDelay] = useState<string>(activeDatabaseName);

    useEffect(() => {
        if (!activeDatabaseName) {
            const timeout = setTimeout(() => {
                setActiveDatabaseNameWithDelay(activeDatabaseName);
            }, 500);

            return () => {
                clearTimeout(timeout);
            };
        } else {
            setActiveDatabaseNameWithDelay(activeDatabaseName);
        }
    }, [activeDatabaseName]);

    return (
        <UncontrolledDropdown direction="down">
            <DropdownToggle caret className="database-switcher">
                <Icon icon="database" />
                {activeDatabaseNameWithDelay || "No database selected"}
                <DropdownMenu className="w-fit-content">
                    {allDatabaseNames.map((databaseName) => (
                        <DropdownItem
                            key={databaseName}
                            onClick={() => {
                                const db = databasesManager.default.getDatabaseByName(databaseName);
                                databasesManager.default.activate(db);
                            }}
                        >
                            {databaseName}
                        </DropdownItem>
                    ))}
                </DropdownMenu>
            </DropdownToggle>
        </UncontrolledDropdown>
    );
}
