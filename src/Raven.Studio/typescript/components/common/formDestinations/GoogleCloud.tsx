import React from "react";
import { Button, Card, CardBody, Collapse, Label, PopoverBody, UncontrolledPopover } from "reactstrap";
import { FormInput, FormSwitch } from "components/common/Form";
import { useFormContext, useWatch } from "react-hook-form";
import OverrideConfiguration from "./OverrideConfiguration";
import { FlexGrow } from "components/common/FlexGrow";
import { Icon } from "components/common/Icon";
import { FormDestinations } from "./formDestinationsUtils";

export default function GoogleCloud() {
    const { control } = useFormContext<FormDestinations>();
    const { googleCloud: formValues } = useWatch({ control });

    return (
        <Card className="well">
            <CardBody>
                <FormSwitch name="googleCloud.isEnabled" control={control}>
                    Google Cloud
                </FormSwitch>
                <Collapse isOpen={formValues.isEnabled} className="mt-2">
                    <FormSwitch
                        name="googleCloud.isOverrideConfig"
                        control={control}
                        className="ms-3 mb-2 w-100"
                        color="secondary"
                    >
                        Override configuration via external script
                    </FormSwitch>
                    {formValues.isOverrideConfig ? (
                        <OverrideConfiguration formName="googleCloud" />
                    ) : (
                        <>
                            <div>
                                <Label className="mb-0 md-label">
                                    Bucket <Icon icon="info" color="info" id="bucketTooltip" />
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
                                    name="googleCloud.bucketName"
                                    control={control}
                                    placeholder="Enter a bucket"
                                    type="text"
                                    className="mb-2"
                                />
                            </div>
                            <div>
                                <Label className="mb-0 md-label">
                                    Remote folder name <small className="text-muted fw-light">(optional)</small>
                                </Label>
                                <FormInput
                                    name="googleCloud.remoteFolderName"
                                    control={control}
                                    placeholder="Enter a remote folder name"
                                    type="text"
                                    className="mb-2"
                                />
                            </div>
                            <div>
                                <Label className="mb-0 md-label">Google Credentials Json</Label>
                                <FormInput
                                    name="googleCloud.googleCredentialsJson"
                                    control={control}
                                    placeholder={googleCredentialsJsonPlaceholder}
                                    type="textarea"
                                    rows={15}
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

const googleCredentialsJsonPlaceholder =
    "e.g.\n" +
    "{\n" +
    '    "type": "service_account",\n' +
    '    "project_id": "test-raven-237012",\n' +
    '    "private_key_id": "12345678123412341234123456789101",\n' +
    '    "private_key": "-----BEGIN PRIVATE KEY-----\\abCse=\\n-----END PRIVATE KEY-----\\n",\n' +
    '    "client_email": "raven@test-raven-237012-237012.iam.gserviceaccount.com",\n' +
    '    "client_id": "111390682349634407434",\n' +
    '    "auth_uri": "https://accounts.google.com/o/oauth2/auth",\n' +
    '    "token_uri": "https://oauth2.googleapis.com/token",\n' +
    '    "auth_provider_x509_cert_url": "https://www.googleapis.com/oauth2/v1/certs",\n' +
    '    "client_x509_cert_url": "https://www.googleapis.com/robot/v1/metadata/x509/viewonly%40test-raven-237012.iam.gserviceaccount.com"\n' +
    "}";
