import { Button, InputGroup, Label } from "reactstrap";
import { FormInput, FormSelect } from "components/common/Form";
import React, { useState } from "react";
import { useForm } from "react-hook-form";
import { Icon } from "components/common/Icon";
import { exhaustiveStringTuple } from "components/utils/common";
import { SelectOption } from "components/common/select/Select";

interface KafkaConnectionStringProps {
    name?: string;
    bootstrapServers?: string[];
    connectionOptions?: string[];
}
const KafkaConnectionString = (props: KafkaConnectionStringProps) => {
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
                <Label className="mb-0 md-label">Bootstrap Servers</Label>
                <FormInput
                    control={control}
                    name="bootstrapServers"
                    type="text"
                    placeholder="Enter comma-separated Bootstrap Servers"
                />
            </div>
            <div>
                <Label className="mb-0 md-label">
                    Connection Options <small className="text-muted fw-light">(optional)</small>
                </Label>
                <div className="hstack gap-3">
                    <FormInput control={control} name="optionKey" type="text" placeholder="Enter an option key" />
                    <FormInput control={control} name="optionValue" type="text" placeholder="Enter an option value" />
                    <Button color="danger" title="Delete connection option">
                        <Icon icon="trash" margin="m-0" />
                    </Button>
                </div>
            </div>
            <div>
                <Button color="primary">
                    <Icon icon="plus" /> Add new connection option
                </Button>
            </div>
        </>
    );
};
export default KafkaConnectionString;
