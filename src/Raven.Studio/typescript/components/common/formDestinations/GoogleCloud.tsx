import React from "react";
import { Badge, Card, CardBody, Collapse, Label, PopoverBody, UncontrolledPopover } from "reactstrap";
import { FormInput, FormSwitch } from "components/common/Form";
import { useFormContext, useWatch } from "react-hook-form";
import OverrideConfiguration from "./OverrideConfiguration";
import { FlexGrow } from "components/common/FlexGrow";
import { Icon } from "components/common/Icon";
import { FormDestinations } from "./utils/formDestinationsTypes";
import ButtonWithSpinner from "../ButtonWithSpinner";
import { useServices } from "components/hooks/useServices";
import { useAsyncCallback } from "react-async-hook";
import { mapGoogleCloudToDto } from "./utils/formDestinationsMapsToDto";
import ConnectionTestResult from "../connectionTests/ConnectionTestResult";

export default function GoogleCloud() {
    const { control, trigger } = useFormContext<FormDestinations>();
    const {
        destinations: { googleCloud: formValues },
    } = useWatch({ control });

    const { manageServerService } = useServices();

    const asyncTest = useAsyncCallback(async () => {
        const isValid = await trigger(fieldBase);
        if (!isValid) {
            return;
        }

        return manageServerService.testPeriodicBackupCredentials("GoogleCloud", mapGoogleCloudToDto(formValues));
    });

    return (
        <Card className="well">
            <CardBody>
                <FormSwitch name={getName("isEnabled")} control={control}>
                    Google Cloud
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
                                    Bucket <Icon icon="info" color="info" id="bucketTooltip" margin="m-0" />
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
                                    target="bucketTooltip"
                                    trigger="hover"
                                    placement="top"
                                    className="bs5"
                                >
                                    <PopoverBody>
                                        Bucket should be created manually in order for this OLAP to work. You can use
                                        the <span className="text-info">Test credentials</span> button to verify its
                                        existence.
                                        <hr className="my-2" />
                                        <a href="https://cloud.google.com/storage/docs/bucket-naming" target="_blank">
                                            <Icon icon="newtab" />
                                            Bucket naming guidelines
                                        </a>
                                    </PopoverBody>
                                </UncontrolledPopover>
                                <FormInput
                                    name={getName("bucketName")}
                                    control={control}
                                    placeholder="Enter a bucket"
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
                                <Label>Google Credentials Json</Label>
                                <FormInput
                                    name={getName("googleCredentialsJson")}
                                    control={control}
                                    placeholder={googleCredentialsJsonPlaceholder}
                                    type="textarea"
                                    autoComplete="off"
                                    rows={15}
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

const googleCredentialsJsonPlaceholder = `e.g.
{
    "type": "service_account",
    "project_id": "test-raven-237012",
    "private_key_id": "12345678123412341234123456789101",
    "private_key": "-----BEGIN PRIVATE KEY-----\\abCse=-----END PRIVATE KEY-----",
    "client_email": "raven@test-raven-237012-237012.iam.gserviceaccount.com",
    "client_id": "111390682349634407434",
    "auth_uri": "https://accounts.google.com/o/oauth2/auth",
    "token_uri": "https://oauth2.googleapis.com/token",
    "auth_provider_x509_cert_url": "https://www.googleapis.com/oauth2/v1/certs",
    "client_x509_cert_url": "https://www.googleapis.com/robot/v1/metadata/x509/viewonly%40test-raven-237012.iam.gserviceaccount.com"
}`;

const fieldBase = "destinations.googleCloud";

type FormFieldNames = keyof FormDestinations["destinations"]["googleCloud"];

function getName(fieldName: FormFieldNames): `${typeof fieldBase}.${FormFieldNames}` {
    return `${fieldBase}.${fieldName}`;
}
