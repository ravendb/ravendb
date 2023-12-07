import React from "react";
import { Card, CardBody, Collapse, Label } from "reactstrap";
import { FormInput, FormSwitch } from "components/common/Form";
import { useFormContext, useWatch } from "react-hook-form";
import OverrideConfiguration from "./OverrideConfiguration";
import { FormDestinations } from "./utils/formDestinationsTypes";

export default function Local() {
    const { control } = useFormContext<FormDestinations>();
    const {
        destinations: { local: formValues },
    } = useWatch({ control });

    return (
        <Card className="well">
            <CardBody>
                <FormSwitch name={getName("isEnabled")} control={control}>
                    Local
                </FormSwitch>
                <Collapse isOpen={formValues?.isEnabled} className="mt-2">
                    <FormSwitch
                        control={control}
                        name={getName("isOverrideConfig")}
                        className="ms-3 mb-2"
                        color="secondary"
                    >
                        Override configuration via external script
                    </FormSwitch>

                    {formValues?.isOverrideConfig ? (
                        <OverrideConfiguration fieldBase={fieldBase} />
                    ) : (
                        <div>
                            <Label className="mb-0 md-label">Folder path</Label>
                            <FormInput
                                type="text"
                                control={control}
                                name={getName("folderPath")}
                                placeholder="Select a destination path"
                            />
                        </div>
                    )}
                </Collapse>
            </CardBody>
        </Card>
    );
}

const fieldBase = "destinations.local";

type FormFieldNames = keyof FormDestinations["destinations"]["local"];

function getName(fieldName: FormFieldNames): `${typeof fieldBase}.${FormFieldNames}` {
    return `${fieldBase}.${fieldName}`;
}
