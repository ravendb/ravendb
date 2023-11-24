import { Form, Label } from "reactstrap";
import { FormInput, FormSelect } from "components/common/Form";
import React from "react";
import { useForm } from "react-hook-form";
import { SelectOption } from "components/common/select/Select";
import { SqlConnection } from "../connectionStringsTypes";

export interface SqlConnectionStringProps {
    connection: SqlConnection;
}

const SqlConnectionString = ({ connection }: SqlConnectionStringProps) => {
    // TODO validation
    const { control, handleSubmit } = useForm<Omit<SqlConnection, "type">>({
        defaultValues: { ..._.omit(connection, "type") },
    });

    // TODO submit
    return (
        <>
            <div>
                <Label className="mb-0 md-label">Name</Label>
                <FormInput
                    control={control}
                    name="Name"
                    type="text"
                    placeholder="Enter a name for the connection string"
                />
            </div>
            <div>
                <Label className="mb-0 md-label">Database</Label>
                <FormSelect
                    control={control}
                    name="FactoryName"
                    options={sqlFactoryOptions}
                    placeholder="Select factory name"
                    isSearchable={false}
                />
            </div>
            <div>
                <Label className="mb-0 md-label">Connection string</Label>
                <FormInput
                    control={control}
                    name="ConnectionString"
                    type="textarea"
                    placeholder="Enter connection string"
                    rows={3}
                />
            </div>
        </>
    );
};

export default SqlConnectionString;

const allSqlFactoryOptions = [
    "Microsoft SQL Server (System.Data.SqlClient)",
    "MySQL Server (MySql.Data.MySqlClient)",
    "MySQL Server (MySqlConnector.MySqlConnectorFactory)",
    "PostgreSQL (Npgsql)",
    "Oracle Database (Oracle.ManagedDataAccess.Client)",
];

const sqlFactoryOptions: SelectOption[] = allSqlFactoryOptions.map((type) => ({
    value: type,
    label: type,
}));
