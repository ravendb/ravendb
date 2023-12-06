import React from "react";
import { InputGroup, InputGroupText, Label } from "reactstrap";
import { FormInput } from "components/common/Form";
import { useFormContext } from "react-hook-form";
import { BackupConfigurationScript, FormDestinations } from "./formDestinationsUtils";

interface OverrideConfigurationProps {
    formName: keyof FormDestinations;
}

const OverrideConfiguration = ({ formName }: OverrideConfigurationProps) => {
    const { control } = useFormContext<FormDestinations>();

    return (
        <>
            <div>
                <Label className="mb-0 md-label">Exec</Label>
                <FormInput
                    name={getName(formName, "exec")}
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
                    name={getName(formName, "arguments")}
                    control={control}
                    placeholder="Command line arguments passed to exec"
                    className="mb-2"
                />
            </div>
            <div>
                <Label className="mb-0 md-label">Timeout</Label>
                <InputGroup>
                    <FormInput
                        name={getName(formName, "timeoutInMs")}
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
    formName: keyof FormDestinations,
    fieldName: keyof BackupConfigurationScript
): `${keyof FormDestinations}.config.${keyof BackupConfigurationScript}` {
    return `${formName}.config.${fieldName}`;
}

export default OverrideConfiguration;
