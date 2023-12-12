import React from "react";
import { Card, CardBody, Collapse, Label, PopoverBody, UncontrolledPopover } from "reactstrap";
import { FormInput, FormSelect, FormSwitch } from "components/common/Form";
import { useFormContext, useWatch } from "react-hook-form";
import OverrideConfiguration from "./OverrideConfiguration";
import { Icon } from "components/common/Icon";
import { exhaustiveStringTuple } from "components/utils/common";
import { SelectOption } from "components/common/select/Select";
import { FlexGrow } from "components/common/FlexGrow";
import { FormDestinations } from "./utils/formDestinationsTypes";
import { useServices } from "components/hooks/useServices";
import { useAsyncCallback } from "react-async-hook";
import { mapFtpToDto } from "./utils/formDestinationsMapsToDto";
import ButtonWithSpinner from "../ButtonWithSpinner";
import ConnectionTestResult from "../connectionTests/ConnectionTestResult";

export default function AmazonGlacier() {
    const { control, trigger } = useFormContext<FormDestinations>();
    const {
        destinations: { glacier: formValues },
    } = useWatch({ control });

    const { manageServerService } = useServices();

    const asyncTest = useAsyncCallback(async () => {
        const isValid = await trigger(fieldBase);
        if (!isValid) {
            return;
        }

        return manageServerService.testPeriodicBackupCredentials("Glacier", mapFtpToDto(formValues));
    });

    return (
        <Card className="well">
            <CardBody>
                <FormSwitch name={getName("isEnabled")} control={control}>
                    Amazon Glacier
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
                                    Vault name
                                    <Icon icon="info" color="info" id="vaultNameTooltip" />
                                </Label>
                                <UncontrolledPopover
                                    target="vaultNameTooltip"
                                    trigger="hover"
                                    placement="top"
                                    className="bs5"
                                >
                                    <PopoverBody>
                                        Vault should be created manually in order for this OLAP to work. You can use the{" "}
                                        <span className="text-info">Test credentials</span> button to verify its
                                        existence.
                                    </PopoverBody>
                                </UncontrolledPopover>
                                <FormInput
                                    name={getName("vaultName")}
                                    control={control}
                                    placeholder="Enter a vault name"
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
                                <Label className="mb-0 md-label">Region</Label>
                                <FormSelect
                                    name={getName("awsRegionName")}
                                    control={control}
                                    placeholder="Select an AWS region"
                                    options={allRegionsOptions}
                                    className="mb-2"
                                />
                            </div>
                            <div>
                                <Label className="mb-0 md-label">Access key</Label>
                                <FormInput
                                    name={getName("awsSecretKey")}
                                    control={control}
                                    placeholder="Enter an access key"
                                    type="text"
                                    className="mb-2"
                                />
                            </div>
                            <div>
                                <Label className="mb-0 md-label">Secret key</Label>
                                <FormInput
                                    name={getName("awsSecretKey")}
                                    control={control}
                                    placeholder="Enter a secret key"
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

const allRegions = exhaustiveStringTuple()("Africa", "Europe", "America", "Asia");

const allRegionsOptions: SelectOption[] = allRegions.map((type) => ({
    value: type,
    label: type,
}));

const fieldBase = "destinations.glacier";

type FormFieldNames = keyof FormDestinations["destinations"]["glacier"];

function getName(fieldName: FormFieldNames): `${typeof fieldBase}.${FormFieldNames}` {
    return `${fieldBase}.${fieldName}`;
}
