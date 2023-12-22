import React from "react";
import { InputGroup, InputGroupText, Label } from "reactstrap";
import { FormInput } from "components/common/Form";
import { useFormContext } from "react-hook-form";
import { BackupConfigurationScript, FormDestinations } from "./utils/formDestinationsTypes";

type FieldBase = `destinations.${keyof FormDestinations["destinations"]}`;

interface OverrideConfigurationProps {
    fieldBase: FieldBase;
}

const OverrideConfiguration = ({ fieldBase }: OverrideConfigurationProps) => {
    const { control } = useFormContext<FormDestinations>();

    return (
        <div className="vstack gap-3 mt-2">
            <div className="mb-2">
                <Label>Exec</Label>
                <FormInput
                    name={getName(fieldBase, "exec")}
                    control={control}
                    placeholder="Path to executable"
                    type="text"
                    autoComplete="off"
                />
            </div>
            <div className="mb-2">
                <Label>Arguments</Label>
                <FormInput
                    type="text"
                    name={getName(fieldBase, "arguments")}
                    control={control}
                    placeholder="Command line arguments passed to exec"
                    autoComplete="off"
                />
            </div>
            <div>
                <Label>Timeout</Label>
                <InputGroup>
                    <FormInput
                        name={getName(fieldBase, "timeoutInMs")}
                        control={control}
                        placeholder="10000 (default)"
                        type="number"
                        autoComplete="off"
                    />
                    <InputGroupText>ms</InputGroupText>
                </InputGroup>
            </div>
        </div>
    );
};

function getName(
    fieldBase: `destinations.${keyof FormDestinations["destinations"]}`,
    fieldName: keyof BackupConfigurationScript
): `destinations.${keyof FormDestinations["destinations"]}.config.${keyof BackupConfigurationScript}` {
    return `${fieldBase}.config.${fieldName}`;
}

export default OverrideConfiguration;
