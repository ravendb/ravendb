import { Label } from "reactstrap";
import { FormInput, FormSelect } from "components/common/Form";
import React from "react";
import { useForm } from "react-hook-form";
import { exhaustiveStringTuple } from "components/utils/common";
import { SelectOption } from "components/common/select/Select";

interface SqlConnectionStringProps {
    name?: string;
    factory?: string;
    connectionString?: string;
}

const allSqlFactoryOptions = exhaustiveStringTuple()(
    "Microsoft SQL Server (System.Data.SqlClient)",
    "MySQL Server (MySql.Data.MySqlClient)",
    "MySQL Server (MySqlConnector.MySqlConnectorFactory)",
    "PostgreSQL (Npgsql)",
    "Oracle Database (Oracle.ManagedDataAccess.Client)"
);

const sqlFactoryOptions: SelectOption[] = allSqlFactoryOptions.map((type) => ({
    value: type,
    label: type,
}));

const SqlConnectionString = (props: SqlConnectionStringProps) => {
    const { control } = useForm<null>({});

    return (
        <>
            <div>
                <Label className="mb-0 md-label">Name</Label>
                <FormInput
                    control={control}
                    name="name"
                    type="text"
                    placeholder="Enter a name for the connection string"
                />
            </div>
            <div>
                <Label className="mb-0 md-label">Database</Label>
                <FormSelect
                    control={control}
                    name="factory"
                    type="text"
                    options={sqlFactoryOptions}
                    placeholder="Select factory name"
                />
            </div>
            <div>
                <Label className="mb-0 md-label">Connection string</Label>
                <FormInput
                    control={control}
                    name="connectionString"
                    type="textarea"
                    placeholder="Enter connection string"
                    rows={3}
                />
            </div>
        </>
    );
};
export default SqlConnectionString;
