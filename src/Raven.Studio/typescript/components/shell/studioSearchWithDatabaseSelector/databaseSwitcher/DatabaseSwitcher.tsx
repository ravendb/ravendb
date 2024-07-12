import "./DatabaseSwitcher.scss";
import React, { useMemo } from "react";
import { databaseSelectors } from "components/common/shell/databaseSliceSelectors";
import { useAppSelector } from "components/store";
import Select from "components/common/select/Select";
import databasesManager from "common/shell/databasesManager";
import NoDatabasePlaceholder from "./bits/NoDatabasePlaceholder";
import DatabaseOptionItem from "./bits/DatabaseOptionItem";
import DatabaseSingleValue from "./bits/DatabaseSingleValue";
import { DatabaseSwitcherOption } from "./databaseSwitcherTypes";

export default function DatabaseSwitcher() {
    const allDatabases = useAppSelector(databaseSelectors.allDatabases);

    // Disabled databases are always at the bottom
    const options: DatabaseSwitcherOption[] = useMemo(
        () =>
            [...allDatabases]
                .sort((a, b) => (a.isDisabled > b.isDisabled ? 1 : -1))
                .map((db) => {
                    return {
                        value: db.name,
                        isSharded: db.isSharded,
                        environment: db.environment,
                        isDisabled: db.isDisabled,
                    };
                }),
        [allDatabases]
    );

    const activeDatabaseName = useAppSelector(databaseSelectors.activeDatabaseName);
    const selectedDatabase = options.find((x) => x.value === activeDatabaseName);

    const handleSelect = (option: DatabaseSwitcherOption) => {
        const db = databasesManager.default.getDatabaseByName(option.value);
        databasesManager.default.activate(db);
    };

    return (
        <Select
            className="database-switcher"
            placeholder={<NoDatabasePlaceholder />}
            value={selectedDatabase}
            options={options}
            components={{ Option: DatabaseOptionItem, SingleValue: DatabaseSingleValue }}
            onChange={handleSelect}
        />
    );
}
