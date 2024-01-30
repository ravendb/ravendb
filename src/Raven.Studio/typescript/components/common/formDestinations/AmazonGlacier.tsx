import React from "react";
import { Badge, Card, CardBody, Collapse, Label, PopoverBody, UncontrolledPopover } from "reactstrap";
import { FormInput, FormSelectCreatable, FormSwitch } from "components/common/Form";
import { useFormContext, useWatch } from "react-hook-form";
import OverrideConfiguration from "./OverrideConfiguration";
import { Icon } from "components/common/Icon";
import { FlexGrow } from "components/common/FlexGrow";
import { FormDestinations } from "./utils/formDestinationsTypes";
import { useServices } from "components/hooks/useServices";
import { useAsyncCallback } from "react-async-hook";
import { mapFtpToDto } from "./utils/formDestinationsMapsToDto";
import ButtonWithSpinner from "../ButtonWithSpinner";
import ConnectionTestResult from "../connectionTests/ConnectionTestResult";
import { availableGlacierRegions } from "./utils/amazonRegions";

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
                <Collapse isOpen={formValues.isEnabled} className="vstack gap-2 mt-2">
                    <FormSwitch
                        name={`${fieldBase}.config.isOverrideConfig`}
                        control={control}
                        className="ms-3 w-100"
                        color="secondary"
                    >
                        Override configuration via external script
                    </FormSwitch>
                    {formValues.config.isOverrideConfig ? (
                        <OverrideConfiguration fieldBase={fieldBase} />
                    ) : (
                        <div className="vstack gap-3 mt-2">
                            <div className="mb-2">
                                <Label className="d-flex align-items-center gap-1">
                                    Vault name
                                    <Icon icon="info" color="info" id="vaultNameTooltip" margin="m-0" />
                                    {asyncTest.result?.Success ? (
                                        <Badge color="success" pill>
                                            <Icon icon="check" />
                                            Successfully connected
                                        </Badge>
                                    ) : asyncTest.result?.Error ? (
                                        <Badge color="danger" pill>
                                            <Icon icon="warning" />
                                            Failed connection
                                        </Badge>
                                    ) : null}
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
                                    autoComplete="off"
                                />
                            </div>
                            <div className="mb-2">
                                <Label>
                                    Remote folder name <small className="text-muted fw-light">(optional)</small>
                                </Label>
                                <FormInput
                                    name={getName("remoteFolderName")}
                                    control={control}
                                    placeholder="Enter a remote folder name"
                                    type="text"
                                    autoComplete="off"
                                />
                            </div>
                            <div className="mb-2">
                                <Label>Region</Label>
                                <FormSelectCreatable
                                    name={getName("awsRegionName")}
                                    control={control}
                                    placeholder="Select an AWS region (or enter new one)"
                                    options={availableGlacierRegions}
                                />
                            </div>
                            <div className="mb-2">
                                <Label>Access key</Label>
                                <FormInput
                                    name={getName("awsAccessKey")}
                                    control={control}
                                    placeholder="Enter an access key"
                                    type="text"
                                    autoComplete="off"
                                />
                            </div>
                            <div className="mb-2">
                                <Label>Secret key</Label>
                                <FormInput
                                    name={getName("awsSecretKey")}
                                    control={control}
                                    placeholder="Enter a secret key"
                                    type="text"
                                    autoComplete="off"
                                />
                            </div>
                            <div className="d-flex justify-content-end">
                                <FlexGrow />
                                <ButtonWithSpinner
                                    type="button"
                                    color="secondary"
                                    onClick={asyncTest.execute}
                                    isSpinning={asyncTest.loading}
                                >
                                    <Icon icon="rocket" />
                                    Test credentials
                                </ButtonWithSpinner>
                            </div>
                            {asyncTest.result?.Error && (
                                <div className="mt-3">
                                    <ConnectionTestResult testResult={asyncTest.result} />
                                </div>
                            )}
                        </div>
                    )}
                </Collapse>
            </CardBody>
        </Card>
    );
}

const fieldBase = "destinations.glacier";

type FormFieldNames = keyof FormDestinations["destinations"]["glacier"];

function getName(fieldName: FormFieldNames): `${typeof fieldBase}.${FormFieldNames}` {
    return `${fieldBase}.${fieldName}`;
}
