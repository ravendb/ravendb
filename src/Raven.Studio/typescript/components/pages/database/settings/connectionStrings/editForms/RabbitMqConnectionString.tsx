import { Label } from "reactstrap";
import { FormInput } from "components/common/Form";
import React from "react";
import { useForm } from "react-hook-form";

interface RabbitMqConnectionStringProps {
    name?: string;
    connectionString?: string;
}
const RabbitMqConnectionString = (props: RabbitMqConnectionStringProps) => {
    const { control } = useForm<any>({});

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
                <Label className="mb-0 md-label">Connection string</Label>
                <FormInput
                    control={control}
                    name="connectionString"
                    type="textarea"
                    rows={3}
                    placeholder="Enter a connection string for RabbitMQ"
                />
            </div>
        </>
    );
};
export default RabbitMqConnectionString;
