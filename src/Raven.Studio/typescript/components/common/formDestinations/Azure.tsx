import React from "react";
import { Card, CardBody, Collapse, Label, PopoverBody, UncontrolledPopover } from "reactstrap";
import { FormInput, FormSwitch } from "components/common/Form";
import { useFormContext, useWatch } from "react-hook-form";
import OverrideConfiguration from "./OverrideConfiguration";
import { FlexGrow } from "components/common/FlexGrow";
import { Icon } from "components/common/Icon";
import { FormDestinations } from "./utils/formDestinationsTypes";
import { useServices } from "components/hooks/useServices";
import { useAsyncCallback } from "react-async-hook";
import { mapAzureToDto } from "./utils/formDestinationsMapsToDto";
import ButtonWithSpinner from "../ButtonWithSpinner";
import ConnectionTestResult from "../connectionTests/ConnectionTestResult";

export default function Azure() {
    const { control, trigger } = useFormContext<FormDestinations>();
    const {
        destinations: { azure: formValues },
    } = useWatch({ control });

    const { manageServerService } = useServices();

    const asyncTest = useAsyncCallback(async () => {
        const isValid = await trigger(fieldBase);
        if (!isValid) {
            return;
        }

        return manageServerService.testPeriodicBackupCredentials("Azure", mapAzureToDto(formValues));
    });

    return (
        <Card className="well">
            <CardBody>
                <FormSwitch name={getName("isEnabled")} control={control}>
                    Azure
                </FormSwitch>
                <Collapse isOpen={formValues.isEnabled} className="mt-2">
                    <FormSwitch
                        name={`${fieldBase}.config.isOverrideConfig`}
                        control={control}
                        className="ms-3 mb-2 w-100"
                        color="secondary"
                    >
                        Override configuration via external script
                    </FormSwitch>
                    {formValues.config.isOverrideConfig ? (
                        <OverrideConfiguration fieldBase={fieldBase} />
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
                                    name={getName("storageContainer")}
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
                                    name={getName("remoteFolderName")}
                                    control={control}
                                    placeholder="Enter a remote folder name"
                                    type="text"
                                    className="mb-2"
                                />
                            </div>
                            <div>
                                <Label className="mb-0 md-label">Account name</Label>
                                <FormInput
                                    name={getName("accountName")}
                                    control={control}
                                    placeholder="Enter an account name"
                                    type="text"
                                    className="mb-2"
                                />
                            </div>
                            <div>
                                <Label className="mb-0 md-label">Account key</Label>
                                <FormInput
                                    name={getName("accountKey")}
                                    control={control}
                                    placeholder="Enter an account key"
                                    type="text"
                                />
                            </div>
                            <div className="d-flex mt-3">
                                <FlexGrow />
                                <ButtonWithSpinner
                                    type="button"
                                    color="info"
                                    onClick={asyncTest.execute}
                                    isSpinning={asyncTest.loading}
                                >
                                    <Icon icon="rocket" />
                                    Test credentials
                                </ButtonWithSpinner>
                            </div>
                            <div className="mt-2">
                                <ConnectionTestResult testResult={asyncTest.result} />
                            </div>
                        </>
                    )}
                </Collapse>
            </CardBody>
        </Card>
    );
}

const fieldBase = "destinations.azure";

type FormFieldNames = keyof FormDestinations["destinations"]["azure"];

function getName(fieldName: FormFieldNames): `${typeof fieldBase}.${FormFieldNames}` {
    return `${fieldBase}.${fieldName}`;
}
