import React from "react";
import { Card, CardBody, Collapse, Label, PopoverBody, UncontrolledPopover } from "reactstrap";
import { FormInput, FormSwitch } from "components/common/Form";
import { useFormContext, useWatch } from "react-hook-form";
import { FlexGrow } from "components/common/FlexGrow";
import { Icon } from "components/common/Icon";
import { FormDestinations } from "./utils/formDestinationsTypes";
import OverrideConfiguration from "./OverrideConfiguration";
import { useServices } from "components/hooks/useServices";
import { useAsyncCallback } from "react-async-hook";
import { mapFtpToDto } from "./utils/formDestinationsMapsToDto";
import ButtonWithSpinner from "../ButtonWithSpinner";
import ConnectionTestResult from "../connectionTests/ConnectionTestResult";

// TODO styling input file

export default function Ftp() {
    const { control, trigger } = useFormContext<FormDestinations>();
    const {
        destinations: { ftp: formValues },
    } = useWatch({ control });

    const { manageServerService } = useServices();

    const asyncTest = useAsyncCallback(async () => {
        const isValid = await trigger(fieldBase);
        if (!isValid) {
            return;
        }

        return manageServerService.testPeriodicBackupCredentials("FTP", mapFtpToDto(formValues));
    });

    const isCertificateVisible = formValues.url?.startsWith("ftps://");

    return (
        <Card className="well">
            <CardBody>
                <FormSwitch name={getName("isEnabled")} control={control}>
                    FTP
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
                                    Host
                                    <Icon icon="info" color="info" id="hostTooltip" />
                                </Label>
                                <UncontrolledPopover
                                    target="hostTooltip"
                                    trigger="hover"
                                    placement="top"
                                    className="bs5"
                                >
                                    <PopoverBody>
                                        To specify the server protocol, prepend the host with protocol identifier (ftp
                                        and ftps are supported). If no protocol is specified the default one (
                                        <code>ftp://</code>) will be used. You can also enter a complete URL e.g.{" "}
                                        <code>ftp://host.name:port/backup-folder/nested-backup-folder</code>
                                    </PopoverBody>
                                </UncontrolledPopover>
                                <FormInput
                                    name={getName("url")}
                                    control={control}
                                    placeholder="Enter a host"
                                    type="text"
                                    className="mb-2"
                                />
                            </div>
                            <div>
                                <Label className="mb-0 md-label">Username</Label>
                                <FormInput
                                    name={getName("userName")}
                                    control={control}
                                    placeholder="Enter a username"
                                    type="text"
                                    className="mb-2"
                                />
                            </div>
                            <div>
                                <Label className="mb-0 md-label">Password</Label>
                                <FormInput
                                    name={getName("password")}
                                    control={control}
                                    placeholder="Enter a password"
                                    type="password"
                                />
                            </div>
                            {isCertificateVisible && (
                                <div>
                                    <Label className="mb-0 md-label">Certificate</Label>
                                    <FormInput type="file" name={getName("certificateAsBase64")} control={control} />
                                </div>
                            )}
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

const fieldBase = "destinations.ftp";

type FormFieldNames = keyof FormDestinations["destinations"]["ftp"];

function getName(fieldName: FormFieldNames): `${typeof fieldBase}.${FormFieldNames}` {
    return `${fieldBase}.${fieldName}`;
}
