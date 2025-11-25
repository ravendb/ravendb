import { Icon } from "components/common/Icon";
import React from "react";
import { RemoteAttachmentsDestinationFormData } from "components/pages/database/settings/remoteAttachments/remoteAttachmentsValidation";
import { UseAsyncReturn } from "react-async-hook";
import { useFormContext } from "react-hook-form";
import { FormGroup, FormInput, FormLabel, FormSelectCreatable, FormSwitch } from "components/common/Form";
import Collapse from "react-bootstrap/Collapse";
import PopoverWithHoverWrapper from "components/common/PopoverWithHoverWrapper";
import Badge from "react-bootstrap/Badge";
import { availableS3Regions } from "components/utils/common";
import ConnectionTestResult from "components/common/connectionTests/ConnectionTestResult";

interface RemoteAttachmentsDestinationFieldsProps {
    asyncTest: UseAsyncReturn<Raven.Server.Web.System.NodeConnectionTestResult, []>;
}

export function RemoteAttachmentsS3Fields({ asyncTest }: RemoteAttachmentsDestinationFieldsProps) {
    const { control, watch } = useFormContext<RemoteAttachmentsDestinationFormData>();
    const s3Values = watch("s3");

    return (
        <div className="vstack mt-3">
            <FormSwitch control={control} name="s3.isUseCustomHost" className="w-100">
                Use a custom S3 host
            </FormSwitch>

            <Collapse in={s3Values?.isUseCustomHost} mountOnEnter unmountOnExit>
                <div>
                    <FormSwitch control={control} name="s3.forcePathStyle" className="w-100 mt-1 mb-2">
                        <span className="d-flex gap-1 align-items-center">
                            Force path style
                            <PopoverWithHoverWrapper
                                message={
                                    <>
                                        Whether to force path-style URLs for S3 objects
                                        <br />
                                        For example:{" "}
                                        <code>
                                            https://{`{Server-URL}`}/{`{Bucket-Name}`}
                                        </code>
                                        <br />
                                        instead of: <code>{`https://{Bucket-Name}.{Server-URL}`}</code>
                                    </>
                                }
                            >
                                <Icon icon="info" color="info" />
                            </PopoverWithHoverWrapper>
                        </span>
                    </FormSwitch>

                    <FormGroup marginClass="mb-1">
                        <FormLabel>Custom server URL</FormLabel>
                        <FormInput
                            control={control}
                            name="s3.customServerUrl"
                            placeholder="Enter a custom server URL"
                            type="text"
                            autoComplete="off"
                        />
                    </FormGroup>
                </div>
            </Collapse>

            <FormGroup marginClass="mt-2 mb-3">
                <FormLabel>Destination identifier</FormLabel>
                <FormInput type="text" name="identifier" placeholder="Destination identifier" control={control} />
            </FormGroup>

            <FormGroup>
                <FormLabel className="d-flex align-items-center gap-1">
                    Bucket name
                    <ConnectionPill asyncTest={asyncTest} />
                </FormLabel>
                <FormInput
                    control={control}
                    name="s3.bucketName"
                    placeholder="Enter a bucket name"
                    type="text"
                    autoComplete="off"
                />
            </FormGroup>

            <FormGroup>
                <FormLabel>
                    Remote folder name <small className="text-muted fw-light">(optional)</small>
                </FormLabel>
                <FormInput
                    control={control}
                    name="s3.remoteFolderName"
                    placeholder="Enter a remote folder name"
                    type="text"
                    autoComplete="off"
                />
            </FormGroup>

            <FormGroup>
                <FormLabel>
                    Region {s3Values?.isUseCustomHost && <small className="text-muted fw-light">(optional)</small>}
                </FormLabel>
                {s3Values?.isUseCustomHost ? (
                    <FormInput
                        type="text"
                        control={control}
                        name="s3.awsRegionName"
                        placeholder="Enter an AWS region"
                        autoComplete="off"
                    />
                ) : (
                    <FormSelectCreatable
                        name="s3.awsRegionName"
                        control={control}
                        placeholder="Select an AWS region (or enter new one)"
                        options={availableS3Regions}
                    />
                )}
            </FormGroup>

            <FormGroup>
                <FormLabel>Access key</FormLabel>
                <FormInput
                    name="s3.awsAccessKey"
                    control={control}
                    placeholder="Enter an access key"
                    type="text"
                    autoComplete="off"
                />
            </FormGroup>

            <FormGroup>
                <FormLabel>Secret key</FormLabel>
                <FormInput
                    name="s3.awsSecretKey"
                    control={control}
                    placeholder="Enter a secret key"
                    type="password"
                    passwordPreview
                    autoComplete="off"
                />
            </FormGroup>

            {asyncTest.result?.Error && (
                <div className="mt-3">
                    <ConnectionTestResult testResult={asyncTest.result} />
                </div>
            )}
        </div>
    );
}

interface ConnectionPillProps {
    asyncTest: UseAsyncReturn<Raven.Server.Web.System.NodeConnectionTestResult, []>;
}
function ConnectionPill({ asyncTest }: ConnectionPillProps) {
    if (!asyncTest.result) {
        return null;
    }

    if (asyncTest.result.Success) {
        return (
            <Badge bg="success" pill>
                <Icon icon="check" />
                Successfully connected
            </Badge>
        );
    }

    return (
        <Badge bg="danger" pill>
            <Icon icon="warning" />
            Failed connection
        </Badge>
    );
}

export function RemoteAttachmentsAzureFields({ asyncTest }: RemoteAttachmentsDestinationFieldsProps) {
    const { control } = useFormContext<RemoteAttachmentsDestinationFormData>();

    return (
        <div className="mt-3">
            <FormGroup>
                <FormLabel>Destination identifier</FormLabel>
                <FormInput type="text" name="identifier" placeholder="Destination identifier" control={control} />
            </FormGroup>

            <FormGroup>
                <FormLabel className="d-flex gap-1 align-items-center">
                    Storage container
                    <ConnectionPill asyncTest={asyncTest} />
                </FormLabel>
                <FormInput
                    name="azure.storageContainer"
                    control={control}
                    placeholder="Enter a storage container"
                    type="text"
                    autoComplete="off"
                />
            </FormGroup>

            <FormGroup>
                <FormLabel>
                    Remote folder name <small className="text-muted fw-light">(optional)</small>
                </FormLabel>
                <FormInput
                    name="azure.remoteFolderName"
                    control={control}
                    placeholder="Enter a remote folder name"
                    type="text"
                    autoComplete="off"
                />
            </FormGroup>

            <FormGroup>
                <FormLabel>Account name</FormLabel>
                <FormInput
                    name="azure.accountName"
                    control={control}
                    placeholder="Enter an account name"
                    type="text"
                    autoComplete="off"
                />
            </FormGroup>

            <FormGroup>
                <FormLabel>Account key</FormLabel>
                <FormInput
                    name="azure.accountKey"
                    control={control}
                    placeholder="Enter an account key"
                    type="password"
                    passwordPreview
                    autoComplete="off"
                />
            </FormGroup>

            {asyncTest.result?.Error && (
                <div className="mt-3">
                    <ConnectionTestResult testResult={asyncTest.result} />
                </div>
            )}
        </div>
    );
}
