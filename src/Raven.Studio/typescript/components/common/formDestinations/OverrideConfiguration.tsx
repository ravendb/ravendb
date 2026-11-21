import React from "react";
import InputGroup from "react-bootstrap/InputGroup";
import { FormGroup, FormInput, FormLabel } from "components/common/Form";
import OptionalLabel from "components/common/OptionalLabel";
import { useFormContext } from "react-hook-form";
import { BackupConfigurationScript, FormDestinations } from "./utils/formDestinationsTypes";
import { useStudioTranslation } from "hooks/useStudioTranslation";

type FieldBase = `destinations.${keyof FormDestinations["destinations"]}`;

interface OverrideConfigurationProps {
    fieldBase: FieldBase;
}

const OverrideConfiguration = ({ fieldBase }: OverrideConfigurationProps) => {
    const { control } = useFormContext<FormDestinations>();
    const t = useStudioTranslation("formDestinations");

    return (
        <div className="vstack gap-3 mt-2">
            <FormGroup marginClass="mb-2">
                <FormLabel>{t("script.exec")}</FormLabel>
                <FormInput
                    name={getName(fieldBase, "exec")}
                    control={control}
                    placeholder={t("script.execPlaceholder")}
                    type="text"
                    autoComplete="off"
                />
            </FormGroup>
            <FormGroup marginClass="mb-2">
                <FormLabel>
                    {t("script.arguments")} <OptionalLabel />
                </FormLabel>
                <FormInput
                    type="text"
                    name={getName(fieldBase, "arguments")}
                    control={control}
                    placeholder={t("script.argumentsPlaceholder")}
                    autoComplete="off"
                />
            </FormGroup>
            <FormGroup marginClass="m-0">
                <FormLabel>
                    {t("script.timeout")} <OptionalLabel />
                </FormLabel>
                <InputGroup>
                    <FormInput
                        name={getName(fieldBase, "timeoutInMs")}
                        control={control}
                        placeholder={t("script.timeoutPlaceholder")}
                        type="number"
                        autoComplete="off"
                    />
                    <InputGroup.Text>ms</InputGroup.Text>
                </InputGroup>
            </FormGroup>
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
