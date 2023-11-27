import { Form, Label, ModalBody, UncontrolledTooltip } from "reactstrap";
import { FormInput, FormSelect } from "components/common/Form";
import React, { useState } from "react";
import { SubmitHandler, useForm, useWatch } from "react-hook-form";
import { SelectOption } from "components/common/select/Select";
import { EditConnectionStringFormProps, SqlConnection } from "../connectionStringsTypes";
import * as yup from "yup";
import { yupResolver } from "@hookform/resolvers/yup";
import { useDispatch } from "react-redux";
import ButtonWithSpinner from "components/common/ButtonWithSpinner";
import { useAppUrls } from "components/hooks/useAppUrls";
import { useServices } from "components/hooks/useServices";
import { connectionStringsActions } from "../store/connectionStringsSlice";
import { tryHandleSubmit } from "components/utils/common";
import ConnectionStringUsedByTasks from "./shared/ConnectionStringUsedByTasks";
import { useAsyncCallback } from "react-async-hook";
import EditConnectionStringFormFooter from "./shared/ConnectionStringFormFooter";
import ConnectionStringTestResult from "./shared/ConnectionStringTestResult";
import { Icon } from "components/common/Icon";
import { PopoverWithHover } from "components/common/PopoverWithHover";

export interface SqlConnectionStringProps extends EditConnectionStringFormProps {
    initialConnection: SqlConnection;
}

export default function SqlConnectionString({ db, initialConnection, isForNewConnection }: SqlConnectionStringProps) {
    const dispatch = useDispatch();

    const { control, handleSubmit, formState } = useForm<FormData>({
        mode: "all",
        defaultValues: _.omit(initialConnection, "Type"),
        resolver: yupSchemaResolver,
    });

    const formValues = useWatch({ control });
    const { forCurrentDatabase } = useAppUrls();
    const { databasesService } = useServices();
    const [syntaxHelpElement, setSyntaxHelpElement] = useState<HTMLElement>();

    const asyncTest = useAsyncCallback(() => {
        return databasesService.testSqlConnectionString(db, formValues.ConnectionString, formValues.FactoryName);
    });

    const isTestButtonDisabled = !formValues.ConnectionString || !formValues.FactoryName;

    const onSave: SubmitHandler<FormData> = async (formData) => {
        return tryHandleSubmit(async () => {
            const newConnection: SqlConnection = {
                ...formData,
                Type: "Sql",
            };

            await databasesService.saveConnectionString(db, newConnection);

            if (isForNewConnection) {
                dispatch(connectionStringsActions.addConnection(newConnection));
            } else {
                dispatch(
                    connectionStringsActions.editConnection({
                        oldName: initialConnection.Name,
                        newConnection,
                    })
                );
            }

            dispatch(connectionStringsActions.closeEditConnectionModal());
        });
    };

    return (
        <Form onSubmit={handleSubmit(onSave)}>
            <ModalBody className="vstack gap-3">
                <div>
                    <Label className="mb-0 md-label">Name</Label>
                    <FormInput
                        control={control}
                        name="Name"
                        type="text"
                        placeholder="Enter a name for the connection string"
                        disabled={!isForNewConnection}
                    />
                </div>
                <div>
                    <Label className="mb-0 md-label">Factory</Label>
                    <FormSelect
                        control={control}
                        name="FactoryName"
                        options={sqlFactoryOptions}
                        placeholder="Select factory name"
                        isSearchable={false}
                    />
                    {formValues.FactoryName && (
                        <>
                            <small ref={setSyntaxHelpElement} className="text-primary">
                                Syntax <Icon icon="help" />
                            </small>
                            <PopoverWithHover target={syntaxHelpElement}>
                                <div className="p-2">{getSyntaxHelp(formValues.FactoryName)}</div>
                            </PopoverWithHover>
                        </>
                    )}
                </div>
                <div>
                    <Label className="mb-0 md-label">Connection string</Label>
                    <FormInput
                        control={control}
                        name="ConnectionString"
                        type="textarea"
                        placeholder={getConnectionStringPlaceholder(formValues.FactoryName)}
                        rows={3}
                    />
                    <div id={testButtonId} className="mt-2" style={{ width: "fit-content" }}>
                        <ButtonWithSpinner
                            color="primary"
                            icon="rocket"
                            onClick={asyncTest.execute}
                            disabled={isTestButtonDisabled}
                            isSpinning={asyncTest.loading}
                        >
                            Test Connection
                        </ButtonWithSpinner>
                    </div>
                    {isTestButtonDisabled && (
                        <UncontrolledTooltip target={testButtonId}>
                            Select a factory and enter a connection string.
                        </UncontrolledTooltip>
                    )}
                </div>
                <ConnectionStringUsedByTasks
                    tasks={initialConnection.UsedByTasks}
                    urlProvider={forCurrentDatabase.editRavenEtl}
                />
                <ConnectionStringTestResult testResult={asyncTest.result} />
            </ModalBody>
            <EditConnectionStringFormFooter isSubmitting={formState.isSubmitting} />
        </Form>
    );
}

const testButtonId = "test-button";

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

const schema = yup
    .object({
        Name: yup.string().nullable().required(),
        ConnectionString: yup.string().nullable().required(),
        FactoryName: yup.string<SqlConnectionStringFactoryName>().nullable().required(),
    })
    .required();

const yupSchemaResolver = yupResolver(schema);
type FormData = Required<yup.InferType<typeof schema>>;
