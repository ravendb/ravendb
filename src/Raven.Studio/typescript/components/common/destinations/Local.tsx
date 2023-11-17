import React, { useState } from "react";
import { Card, CardBody, Collapse, InputGroup, InputGroupText, Label } from "reactstrap";
import { FormInput, FormSwitch } from "components/common/Form";
import { useForm } from "react-hook-form";
import OverrideConfiguration from "components/common/destinations/OverrideConfiguration";

interface LocalProps {
    className?: string;
}
const Local = (props: LocalProps) => {
    const { className } = props;
    const { control } = useForm<any>({});
    const [isOpen, setIsOpen] = useState(false);
    const [isOverrideConfigurationEnabled, setOverrideConfiguration] = useState(false);
    const toggle = () => setIsOpen(!isOpen);
    const toggleOverrideConfiguration = () => setOverrideConfiguration(!isOverrideConfigurationEnabled);
    return (
        <div className={className}>
            <Card className="well">
                <CardBody>
                    <FormSwitch name="local" control={control} onChange={toggle}>
                        Local
                    </FormSwitch>
                    <Collapse isOpen={isOpen} className="mt-2">
                        <FormSwitch
                            name="overrideConfiguration"
                            control={control}
                            className="ms-3 mb-2 w-100"
                            onChange={toggleOverrideConfiguration}
                            color="secondary"
                        >
                            Override configuration via external script
                        </FormSwitch>
                        {!isOverrideConfigurationEnabled && (
                            <div>
                                <Label className="mb-0 md-label">Folder path</Label>
                                <FormInput
                                    type="text"
                                    name="folderPath"
                                    control={control}
                                    placeholder="Select a destination path"
                                />
                            </div>
                        )}
                        {isOverrideConfigurationEnabled && <OverrideConfiguration />}
                    </Collapse>
                </CardBody>
            </Card>
        </div>
    );
};

export default Local;
