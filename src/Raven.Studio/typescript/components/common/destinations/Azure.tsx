import React, { useState } from "react";
import { Button, Card, CardBody, Collapse, Label, PopoverBody, UncontrolledPopover } from "reactstrap";
import { FormInput, FormSwitch } from "components/common/Form";
import { useForm } from "react-hook-form";
import OverrideConfiguration from "components/common/destinations/OverrideConfiguration";
import { FlexGrow } from "components/common/FlexGrow";
import { Icon } from "components/common/Icon";

interface AzureProps {
    className?: string;
}
const Azure = (props: AzureProps) => {
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
                    <FormSwitch name="azure" control={control} onChange={toggle}>
                        Azure
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
                                            Storage container should be created manually in order for this OLAP to work.
                                            You can use the <span className="text-info">Test credentials</span> button
                                            to verify its existence.
                                        </PopoverBody>
                                    </UncontrolledPopover>
                                    <FormInput
                                        name="storageContainer"
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
                                        name="storageContainer"
                                        control={control}
                                        placeholder="Enter a remote folder name"
                                        type="text"
                                        className="mb-2"
                                    />
                                </div>
                                <div>
                                    <Label className="mb-0 md-label">Account name</Label>
                                    <FormInput
                                        name="accountName"
                                        control={control}
                                        placeholder="Enter an account name"
                                        type="text"
                                        className="mb-2"
                                    />
                                </div>
                                <div>
                                    <Label className="mb-0 md-label">Account key</Label>
                                    <FormInput
                                        name="accountKey"
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
                        {isOverrideConfigurationEnabled && <OverrideConfiguration />}
                    </Collapse>
                </CardBody>
            </Card>
        </div>
    );
};

export default Azure;
