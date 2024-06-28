import { Alert, Badge, Form, Label } from "reactstrap";
import { FormInput, FormSelect } from "components/common/Form";
import React, { useState } from "react";
import { SubmitHandler, useForm, useWatch } from "react-hook-form";
import { OptionWithWarning, SelectOptionWithWarning } from "components/common/select/Select";
import { ConnectionFormData, EditConnectionStringFormProps, SqlConnection } from "../connectionStringsTypes";
import * as yup from "yup";
import { yupResolver } from "@hookform/resolvers/yup";
import ButtonWithSpinner from "components/common/ButtonWithSpinner";
import { useAppUrls } from "components/hooks/useAppUrls";
import { useServices } from "components/hooks/useServices";
import ConnectionStringUsedByTasks from "./shared/ConnectionStringUsedByTasks";
import { useAsyncCallback } from "react-async-hook";
import ConnectionTestResult from "../../../../../common/connectionTests/ConnectionTestResult";
import { Icon } from "components/common/Icon";
import { PopoverWithHover } from "components/common/PopoverWithHover";
import { yupObjectSchema } from "components/utils/yupUtils";
import { FlexGrow } from "components/common/FlexGrow";
import { databaseSelectors } from "components/common/shell/databaseSliceSelectors";
import { useAppSelector } from "components/store";

type FormData = ConnectionFormData<SqlConnection>;

export interface SqlConnectionStringProps extends EditConnectionStringFormProps {
    initialConnection: SqlConnection;
}

export default function SqlConnectionString({
    initialConnection,
    isForNewConnection,
    onSave,
}: SqlConnectionStringProps) {
    const databaseName = useAppSelector(databaseSelectors.activeDatabaseName);
    const { control, handleSubmit, trigger } = useForm<FormData>({
        mode: "all",
        defaultValues: getDefaultValues(initialConnection, isForNewConnection),
        resolver: yupSchemaResolver,
    });

    const formValues = useWatch({ control });
    const { forCurrentDatabase } = useAppUrls();
    const { tasksService } = useServices();
    const [syntaxHelpElement, setSyntaxHelpElement] = useState<HTMLElement>();

    const asyncTest = useAsyncCallback(async () => {
        const isValid = await trigger(["connectionString", "factoryName"]);
        if (!isValid) {
            return;
        }

        return tasksService.testSqlConnectionString(databaseName, formValues.connectionString, formValues.factoryName);
    });

    const handleSave: SubmitHandler<FormData> = (formData: FormData) => {
        onSave({
            type: "Sql",
            ...formData,
        } satisfies SqlConnection);
    };

    return (
        <Form id="connection-string-form" onSubmit={handleSubmit(handleSave)} className="vstack gap-3">
            <div className="mb-2">
                <Label>Name</Label>
                <FormInput
                    control={control}
                    name="name"
                    type="text"
                    placeholder="Enter a name for the connection string"
                    disabled={!isForNewConnection}
                    autoComplete="off"
                />
            </div>
            <div className="mb-2">
                <div className="d-flex flex-grow align-items-baseline justify-content-between">
                    <Label>Factory</Label>
                    {formValues.factoryName && (
                        <>
                            <small ref={setSyntaxHelpElement} className="text-primary">
                                Syntax <Icon icon="help" margin="m-0" />
                            </small>
                            <PopoverWithHover target={syntaxHelpElement}>
                                <div className="p-2">{getSyntaxHelp(formValues.factoryName)}</div>
                            </PopoverWithHover>
                        </>
                    )}
                </div>
                <FormSelect
                    control={control}
                    name="factoryName"
                    options={sqlFactoryOptions}
                    placeholder="Select factory name"
                    isSearchable={false}
                    components={{ Option: OptionWithWarning }}
                />
                {formValues.factoryName === "MySql.Data.MySqlClient" && (
                    <Alert color="warning mt-1">
                        <Icon icon="warning" color="warning" />
                        This connector is deprecated. MySqlConnector will be used instead. Please update Factory to:
                        MySqlConnector.MySqlConnectorFactory.
                    </Alert>
                )}
            </div>
            <div className="mb-2">
                <Label className="d-flex align-items-center gap-1">
                    Connection string{" "}
                    {asyncTest.result?.Success ? (
                        <Badge color="success" pill>
                            <Icon icon="check" />
                            Successfully connected
                        </Badge>
                    ) : asyncTest.result?.Error ? (
                        <Badge color="danger" pill>
                            <Icon icon="warning" />
                            Failed connection
                        </Badge>
                    ) : null}
                </Label>
                <FormInput
                    control={control}
                    name="connectionString"
                    type="textarea"
                    placeholder={getConnectionStringPlaceholder(formValues.factoryName)}
                    rows={3}
                    autoComplete="off"
                />
                <div className="d-flex mt-4">
                    <FlexGrow />
                    <ButtonWithSpinner
                        color="secondary"
                        icon="rocket"
                        onClick={asyncTest.execute}
                        isSpinning={asyncTest.loading}
                    >
                        Test connection
                    </ButtonWithSpinner>
                </div>
            </div>
            <ConnectionStringUsedByTasks
                tasks={initialConnection.usedByTasks}
                urlProvider={forCurrentDatabase.editSqlEtl}
            />
            {asyncTest.result?.Error && <ConnectionTestResult testResult={asyncTest.result} />}
        </Form>
    );
}

const sqlFactoryOptions: SelectOptionWithWarning<SqlConnectionStringFactoryName>[] = [
    { value: "System.Data.SqlClient", label: "Microsoft SQL Server (System.Data.SqlClient)" },
    { value: "MySqlConnector.MySqlConnectorFactory", label: "MySQL Server (MySqlConnector.MySqlConnectorFactory)" },
    { value: "Npgsql", label: "PostgreSQL (Npgsql)" },
    { value: "Oracle.ManagedDataAccess.Client", label: "Oracle Database (Oracle.ManagedDataAccess.Client)" },
    { value: "MySql.Data.MySqlClient", label: "DEPRECATED: MySQL Server (MySql.Data.MySqlClient)", isWarning: true },
];

function getSyntaxHelp(factory: SqlConnectionStringFactoryName) {
    switch (factory) {
        case "System.Data.SqlClient":
            return (
                <span>
                    Example: <code>Data Source=10.0.0.107;Database=SourceDB;User ID=sa;Password=secret;</code>
                    <br />
                    <span>
                        More examples can be found in{" "}
                        <a href="https://ravendb.net/l/38S9OQ" target="_blank">
                            full syntax reference
                            <Icon icon="link" margin="ms-1" />
                        </a>
                    </span>
                </span>
            );
        case "MySql.Data.MySqlClient":
        case "MySqlConnector.MySqlConnectorFactory":
            return (
                <span>
                    Example: <code>server=10.0.0.103;port=3306;userid=root;password=secret;</code>
                    <br />
                    <span>
                        More examples can be found in{" "}
                        <a href="https://ravendb.net/l/BSS8YH" target="_blank">
                            full syntax reference
                            <Icon icon="link" margin="ms-1" />
                        </a>
                    </span>
                </span>
            );
        case "Npgsql":
            return (
                <span>
                    Example: <code>Host=10.0.0.105;Port=5432;Username=postgres;Password=secret</code>
                    <br />
                    <span>
                        More examples can be found in{" "}
                        <a href="https://ravendb.net/l/FWEBWD" target="_blank">
                            full syntax reference
                            <Icon icon="link" margin="ms-1" />
                        </a>
                    </span>
                </span>
            );
        case "Oracle.ManagedDataAccess.Client":
            return (
                <span>
                    Example:{" "}
                    <code>
                        Data
                        Source=(DESCRIPTION=(ADDRESS_LIST=(ADDRESS=(PROTOCOL=TCP)(HOST=10.0.0.101)(PORT=1521)))(CONNECT_DATA=(SID=ORCLCDB)));User
                        Id=SYS;DBA Privilege=SYSDBA;password=secret;
                    </code>
                    <br />
                    <span>
                        More examples can be found in{" "}
                        <a href="https://ravendb.net/l/TG851N" target="_blank">
                            full syntax reference
                            <Icon icon="link" margin="ms-1" />
                        </a>
                    </span>
                </span>
            );
        default:
            return null;
    }
}

function getConnectionStringPlaceholder(factoryName: SqlConnectionStringFactoryName) {
    const optionLabel = sqlFactoryOptions.find((x) => x.value === factoryName)?.label;

    return optionLabel ? `Enter the complete connection string for the ${optionLabel}` : "Enter connection string";
}

const schema = yupObjectSchema<FormData>({
    name: yup.string().nullable().required(),
    connectionString: yup.string().nullable().required(),
    factoryName: yup.string<SqlConnectionStringFactoryName>().nullable().required(),
});

const yupSchemaResolver = yupResolver(schema);

function getDefaultValues(initialConnection: SqlConnection, isForNewConnection: boolean): FormData {
    if (isForNewConnection) {
        return {
            name: null,
            factoryName: null,
            connectionString: null,
        };
    }

    return _.omit(initialConnection, "type", "usedByTasks");
}
