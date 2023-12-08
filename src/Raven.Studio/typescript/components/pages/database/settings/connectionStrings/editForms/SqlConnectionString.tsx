import { Form, Label } from "reactstrap";
import { FormInput, FormSelect } from "components/common/Form";
import React, { useState } from "react";
import { SubmitHandler, useForm, useWatch } from "react-hook-form";
import { SelectOption } from "components/common/select/Select";
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

type FormData = ConnectionFormData<SqlConnection>;

export interface SqlConnectionStringProps extends EditConnectionStringFormProps {
    initialConnection: SqlConnection;
}

export default function SqlConnectionString({
    db,
    initialConnection,
    isForNewConnection,
    onSave,
}: SqlConnectionStringProps) {
    const { control, handleSubmit, trigger } = useForm<FormData>({
        mode: "all",
        defaultValues: getDefaultValues(initialConnection, isForNewConnection),
        resolver: yupSchemaResolver,
    });

    const formValues = useWatch({ control });
    const { forCurrentDatabase } = useAppUrls();
    const { databasesService } = useServices();
    const [syntaxHelpElement, setSyntaxHelpElement] = useState<HTMLElement>();

    const asyncTest = useAsyncCallback(async () => {
        const isValid = await trigger(["connectionString", "factoryName"]);
        if (!isValid) {
            return;
        }

        return databasesService.testSqlConnectionString(db, formValues.connectionString, formValues.factoryName);
    });

    const handleSave: SubmitHandler<FormData> = (formData: FormData) => {
        onSave({
            type: "Sql",
            ...formData,
        } satisfies SqlConnection);
    };

    return (
        <Form id="connection-string-form" onSubmit={handleSubmit(handleSave)} className="vstack gap-2">
            <div>
                <Label className="mb-0 md-label">Name</Label>
                <FormInput
                    control={control}
                    name="name"
                    type="text"
                    placeholder="Enter a name for the connection string"
                    disabled={!isForNewConnection}
                />
            </div>
            <div>
                <Label className="mb-0 md-label">Factory</Label>
                <FormSelect
                    control={control}
                    name="factoryName"
                    options={sqlFactoryOptions}
                    placeholder="Select factory name"
                    isSearchable={false}
                />
                {formValues.factoryName && (
                    <>
                        <small ref={setSyntaxHelpElement} className="text-primary">
                            Syntax <Icon icon="help" />
                        </small>
                        <PopoverWithHover target={syntaxHelpElement}>
                            <div className="p-2">{getSyntaxHelp(formValues.factoryName)}</div>
                        </PopoverWithHover>
                    </>
                )}
            </div>
            <div>
                <Label className="mb-0 md-label">Connection string</Label>
                <FormInput
                    control={control}
                    name="connectionString"
                    type="textarea"
                    placeholder={getConnectionStringPlaceholder(formValues.factoryName)}
                    rows={3}
                />
                <ButtonWithSpinner
                    className="mt-2"
                    color="primary"
                    icon="rocket"
                    onClick={asyncTest.execute}
                    isSpinning={asyncTest.loading}
                >
                    Test Connection
                </ButtonWithSpinner>
            </div>
            <ConnectionStringUsedByTasks
                tasks={initialConnection.usedByTasks}
                urlProvider={forCurrentDatabase.editSqlEtl}
            />
            <ConnectionTestResult testResult={asyncTest.result} />
        </Form>
    );
}

const sqlFactoryOptions: SelectOption<SqlConnectionStringFactoryName>[] = [
    { value: "System.Data.SqlClient", label: "Microsoft SQL Server" },
    { value: "MySql.Data.MySqlClient", label: "MySQL Server" },
    { value: "MySqlConnector.MySqlConnectorFactory", label: "MySQL Server" },
    { value: "Npgsql", label: "PostgreSQL" },
    { value: "Oracle.ManagedDataAccess.Client", label: "Oracle Database" },
];

function getSyntaxHelp(factory: SqlConnectionStringFactoryName) {
    switch (factory) {
        case "System.Data.SqlClient":
            return (
                <span>
                    Example: <code>Data Source=10.0.0.107;Database=SourceDB;User ID=sa;Password=secret;</code>
                    <br />
                    <small>
                        More examples can be found in{" "}
                        <a href="https://ravendb.net/l/38S9OQ" target="_blank">
                            <Icon icon="link" margin="m-0" />
                            full syntax reference
                        </a>
                    </small>
                </span>
            );
        case "MySql.Data.MySqlClient":
        case "MySqlConnector.MySqlConnectorFactory":
            return (
                <span>
                    Example: <code>server=10.0.0.103;port=3306;userid=root;password=secret;</code>
                    <br />
                    <small>
                        More examples can be found in{" "}
                        <a href="https://ravendb.net/l/BSS8YH" target="_blank">
                            <Icon icon="link" margin="m-0" />
                            full syntax reference
                        </a>
                    </small>
                </span>
            );
        case "Npgsql":
            return (
                <span>
                    Example: <code>Host=10.0.0.105;Port=5432;Username=postgres;Password=secret</code>
                    <br />
                    <small>
                        More examples can be found in{" "}
                        <a href="https://ravendb.net/l/FWEBWD" target="_blank">
                            <Icon icon="link" margin="m-0" />
                            full syntax reference
                        </a>
                    </small>
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
                    <small>
                        More examples can be found in{" "}
                        <a href="https://ravendb.net/l/TG851N" target="_blank">
                            <Icon icon="link" margin="m-0" />
                            full syntax reference
                        </a>
                    </small>
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
