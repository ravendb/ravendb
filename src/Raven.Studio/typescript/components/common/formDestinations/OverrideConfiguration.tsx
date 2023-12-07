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
        <>
            <div>
                <Label className="mb-0 md-label">Exec</Label>
                <FormInput
                    name={getName(fieldBase, "exec")}
                    control={control}
                    placeholder="Path to executable"
                    className="mb-2"
                    type="text"
                />
            </div>
            <div>
                <Label className="mb-0 md-label">Arguments</Label>
                <FormInput
                    type="text"
                    name={getName(fieldBase, "arguments")}
                    control={control}
                    placeholder="Command line arguments passed to exec"
                    className="mb-2"
                />
            </div>
            <div>
                <Label className="mb-0 md-label">Timeout</Label>
                <InputGroup>
                    <FormInput
                        name={getName(fieldBase, "timeoutInMs")}
                        control={control}
                        placeholder="10000 (default)"
                        type="number"
                    />
                    <InputGroupText>ms</InputGroupText>
                </InputGroup>
            </div>
        </>
    );
};

function getName(
    fieldBase: `destinations.${keyof FormDestinations["destinations"]}`,
    fieldName: keyof BackupConfigurationScript
): `destinations.${keyof FormDestinations["destinations"]}.config.${keyof BackupConfigurationScript}` {
    return `${fieldBase}.config.${fieldName}`;
}

export default OverrideConfiguration;
