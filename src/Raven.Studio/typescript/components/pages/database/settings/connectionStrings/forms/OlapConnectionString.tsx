import { Label } from "reactstrap";
import { FormInput } from "components/common/Form";
import React from "react";
import { useForm } from "react-hook-form";
import Destinations from "components/common/destinations/Destinations";

interface OlapConnectionStringProps {
    name?: string;
}

const OlapConnectionString = (props: OlapConnectionStringProps) => {
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
                <Destinations />
            </div>
        </>
    );
};
export default OlapConnectionString;
