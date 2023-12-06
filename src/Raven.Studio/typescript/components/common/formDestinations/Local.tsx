import React from "react";
import { Card, CardBody, Collapse, Label } from "reactstrap";
import { FormInput, FormSwitch } from "components/common/Form";
import { useFormContext, useWatch } from "react-hook-form";
import OverrideConfiguration from "./OverrideConfiguration";
import { FormDestinations } from "./formDestinationsUtils";

export default function Local() {
    const { control } = useFormContext<FormDestinations>();
    const { local: formValues } = useWatch({ control });

    return (
        <Card className="well">
            <CardBody>
                <FormSwitch name="local.isEnabled" control={control}>
                    Local
                </FormSwitch>
                <Collapse isOpen={formValues?.isEnabled} className="mt-2">
                    <FormSwitch control={control} name="local.isOverrideConfig" className="ms-3 mb-2" color="secondary">
                        Override configuration via external script
                    </FormSwitch>

                    {formValues?.isOverrideConfig ? (
                        <OverrideConfiguration formName="local" />
                    ) : (
                        <div>
                            <Label className="mb-0 md-label">Folder path</Label>
                            <FormInput
                                type="text"
                                control={control}
                                name="local.folderPath"
                                placeholder="Select a destination path"
                            />
                        </div>
                    )}
                </Collapse>
            </CardBody>
        </Card>
    );
}
