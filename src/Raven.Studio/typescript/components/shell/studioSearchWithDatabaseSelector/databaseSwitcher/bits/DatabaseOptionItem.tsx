import DatabaseEnvironmentBadge from "components/shell/studioSearchWithDatabaseSelector/databaseSwitcher/bits/DatabaseEnvironmentBadge";
import DatabaseIcon from "components/shell/studioSearchWithDatabaseSelector/databaseSwitcher/bits/DatabaseIcon";
import DisabledBadge from "components/shell/studioSearchWithDatabaseSelector/databaseSwitcher/bits/DisabledBadge";
import { DatabaseSwitcherOption } from "components/shell/studioSearchWithDatabaseSelector/databaseSwitcher/databaseSwitcherTypes";
import React from "react";
import { components, OptionProps } from "react-select";

export default function DatabaseOptionItem(props: OptionProps<DatabaseSwitcherOption>) {
    const { data } = props;

    return (
        <components.Option {...props}>
            <div className="d-flex align-items-center">
                <DatabaseIcon databaseName={data.value} isSharded={data.isSharded} />
                <div className="database-name">{data.value}</div>
                <DisabledBadge isDisabled={data.isDisabled} />
                <DatabaseEnvironmentBadge environment={data.environment} />
            </div>
        </components.Option>
    );
}
