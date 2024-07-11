import "./DatabaseSwitcher.scss";
import React, { useMemo } from "react";
import { Icon } from "components/common/Icon";
import { databaseSelectors } from "components/common/shell/databaseSliceSelectors";
import { useAppSelector } from "components/store";
import { Badge } from "reactstrap";
import IconName from "typings/server/icons";
import { TextColor } from "components/models/common";
import assertUnreachable from "components/utils/assertUnreachable";
import Select from "components/common/select/Select";
import { components, OptionProps, SingleValueProps } from "react-select";
import databasesManager from "common/shell/databasesManager";
type StudioEnvironment = Raven.Client.Documents.Operations.Configuration.StudioConfiguration.StudioEnvironment;

export default function DatabaseSwitcher() {
    const allDatabases = useAppSelector(databaseSelectors.allDatabases);

    // Disabled databases are always at the bottom
    const options: DatabaseOption[] = useMemo(
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

    const handleSelect = (option: DatabaseOption) => {
        const db = databasesManager.default.getDatabaseByName(option.value);
        databasesManager.default.activate(db);
    };

    return (
        <Select
            className="database-switcher"
            placeholder={<NoDatabasePlaceholder />}
            value={selectedDatabase}
            options={options}
            components={{ Option: DatabaseOptionItem, SingleValue: DatabaseValue }}
            onChange={handleSelect}
        />
    );
}

function NoDatabasePlaceholder() {
    return (
        <span>
            <Icon icon="database" addon="cancel" />
            No database selected
        </span>
    );
}

interface DatabaseOption {
    value: string;
    isSharded: boolean;
    environment: StudioEnvironment;
    isDisabled: boolean;
}

function DatabaseOptionItem(props: OptionProps<DatabaseOption>) {
    const { data } = props;

    return (
        <components.Option {...props}>
            <DatabaseIcon databaseName={data.value} isSharded={data.isSharded} />
            <span>{data.value}</span>
            <DisabledBadge isDisabled={data.isDisabled} />
            <ServerEnvironmentBadge environment={data.environment} />
        </components.Option>
    );
}

function DatabaseValue({ ...props }: SingleValueProps<DatabaseOption>) {
    const { data } = props;

    return (
        <components.SingleValue {...props}>
            <DatabaseIcon databaseName={data.value} isSharded={data.isSharded} />
            <span>{data.value}</span>
            <DisabledBadge isDisabled={data.isDisabled} />
            <ServerEnvironmentBadge environment={data.environment} />
        </components.SingleValue>
    );
}

function ServerEnvironmentBadge({ environment }: { environment: StudioEnvironment }) {
    if (!environment || environment === "None") {
        return null;
    }

    const getColor = () => {
        switch (environment) {
            case "Production":
                return "danger";
            case "Testing":
                return "success";
            case "Development":
                return "info";
            default:
                return assertUnreachable(environment);
        }
    };

    return (
        <Badge className="ms-2 text-uppercase" color={getColor()} pill>
            {environment}
        </Badge>
    );
}

function DisabledBadge({ isDisabled }: { isDisabled: boolean }) {
    if (!isDisabled) {
        return null;
    }

    return (
        <Badge className="ms-2" pill>
            Disabled
        </Badge>
    );
}

interface DatabaseIconProps {
    databaseName: string;
    isSharded: boolean;
}

function DatabaseIcon({ databaseName, isSharded }: DatabaseIconProps) {
    if (!databaseName) {
        return <Icon icon="database" addon="cancel" />;
    }

    const addon: IconName = isSharded ? "sharding" : null;
    const color: TextColor = isSharded ? "shard" : "primary";

    return <Icon icon="database" addon={addon} color={color} />;
}
