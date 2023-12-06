import React from "react";
import { Button, Card, CardBody, Collapse, Label, PopoverBody, UncontrolledPopover } from "reactstrap";
import { FormInput, FormSwitch } from "components/common/Form";
import { useFormContext, useWatch } from "react-hook-form";
import OverrideConfiguration from "./OverrideConfiguration";
import { FlexGrow } from "components/common/FlexGrow";
import { Icon } from "components/common/Icon";
import { FormDestinations } from "./formDestinationsUtils";

export default function Azure() {
    const { control } = useFormContext<FormDestinations>();
    const { azure: formValues } = useWatch({ control });

    return (
        <Card className="well">
            <CardBody>
                <FormSwitch name="azure.isEnabled" control={control}>
                    Azure
                </FormSwitch>
                <Collapse isOpen={formValues.isEnabled} className="mt-2">
                    <FormSwitch
                        name="azure.isOverrideConfig"
                        control={control}
                        className="ms-3 mb-2 w-100"
                        color="secondary"
                    >
                        Override configuration via external script
                    </FormSwitch>
                    {formValues.isOverrideConfig ? (
                        <OverrideConfiguration formName="azure" />
                    ) : (
                        <>
                            <div>
                                <Label className="mb-0 md-label">
                                    Storage container <Icon icon="info" color="info" id="storageContainerTooltip" />
                                </Label>
                                <UncontrolledPopover
                                    target="storageContainerTooltip"
                                    trigger="hover"
                                    placement="top"
                                    className="bs5"
                                >
                                    <PopoverBody>
                                        Storage container should be created manually in order for this OLAP to work. You
                                        can use the <span className="text-info">Test credentials</span> button to verify
                                        its existence.
                                    </PopoverBody>
                                </UncontrolledPopover>
                                <FormInput
                                    name="azure.storageContainer"
                                    control={control}
                                    placeholder="Enter a storage container"
                                    type="text"
                                    className="mb-2"
                                />
                            </div>
                            <div>
                                <Label className="mb-0 md-label">
                                    Remote folder name <small className="text-muted fw-light">(optional)</small>
                                </Label>
                                <FormInput
                                    name="azure.remoteFolderName"
                                    control={control}
                                    placeholder="Enter a remote folder name"
                                    type="text"
                                    className="mb-2"
                                />
                            </div>
                            <div>
                                <Label className="mb-0 md-label">Account name</Label>
                                <FormInput
                                    name="azure.accountName"
                                    control={control}
                                    placeholder="Enter an account name"
                                    type="text"
                                    className="mb-2"
                                />
                            </div>
                            <div>
                                <Label className="mb-0 md-label">Account key</Label>
                                <FormInput
                                    name="azure.accountKey"
                                    control={control}
                                    placeholder="Enter an account key"
                                    type="text"
                                />
                            </div>
                            <div className="d-flex mt-3">
                                <FlexGrow />
                                <Button color="info">
                                    <Icon icon="rocket" />
                                    Test credentials
                                </Button>
                            </div>
                        </>
                    )}
                </Collapse>
            </CardBody>
        </Card>
    );
}
