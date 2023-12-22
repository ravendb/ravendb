import React, { ChangeEvent } from "react";
import {
    Badge,
    Card,
    CardBody,
    Collapse,
    InputGroup,
    InputGroupText,
    Label,
    PopoverBody,
    UncontrolledPopover,
} from "reactstrap";
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
import fileImporter from "common/fileImporter";

export default function Ftp() {
    const { control, trigger, setValue, formState } = useFormContext<FormDestinations>();
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

    const isCertificateFieldVisible = formValues.url?.startsWith("ftps://");

    const selectFile = (event: ChangeEvent<HTMLInputElement>) => {
        fileImporter.readAsArrayBuffer(event.currentTarget, (data) => {
            let binary = "";
            const bytes = new Uint8Array(data);
            for (let i = 0; i < bytes.byteLength; i++) {
                binary += String.fromCharCode(bytes[i]);
            }
            const result = window.btoa(binary);
            setValue(getName("certificateAsBase64"), result);
        });
    };

    return (
        <Card className="well mb-2">
            <CardBody>
                <FormSwitch name={getName("isEnabled")} control={control}>
                    FTP
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
                                    Host
                                    <Icon icon="info" color="info" id="hostTooltip" margin="m-0" />
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
                                    autoComplete="off"
                                />
                            </div>
                            <div className="mb-2">
                                <Label>Username</Label>
                                <FormInput
                                    name={getName("userName")}
                                    control={control}
                                    placeholder="Enter a username"
                                    type="text"
                                    autoComplete="off"
                                />
                            </div>
                            <div className="mb-2">
                                <Label>Password</Label>
                                <FormInput
                                    name={getName("password")}
                                    control={control}
                                    placeholder="Enter a password"
                                    type="password"
                                    autoComplete="off"
                                    passwordPreview
                                />
                            </div>
                            {isCertificateFieldVisible && (
                                <div className="mb-2">
                                    <Label>Certificate</Label>
                                    <input id="filePicker" type="file" onChange={selectFile} className="d-none" />
                                    <InputGroup>
                                        <span className="static-name form-control d-flex align-items-center">
                                            {formValues.certificateAsBase64 ? "<certificate>" : "Select file..."}
                                        </span>
                                        <InputGroupText>
                                            <label htmlFor="filePicker" className="cursor-pointer">
                                                <Icon icon="document" />
                                                <span>Browse</span>
                                            </label>
                                        </InputGroupText>
                                    </InputGroup>
                                    {formState.errors.destinations?.ftp?.certificateAsBase64 && (
                                        <div className="position-absolute badge bg-danger rounded-pill margin-top-xxs">
                                            {formState.errors.destinations.ftp.certificateAsBase64.message}
                                        </div>
                                    )}
                                </div>
                            )}
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

const fieldBase = "destinations.ftp";

type FormFieldNames = keyof FormDestinations["destinations"]["ftp"];

function getName(fieldName: FormFieldNames): `${typeof fieldBase}.${FormFieldNames}` {
    return `${fieldBase}.${fieldName}`;
}
