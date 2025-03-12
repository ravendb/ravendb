import { ChangeEvent } from "react";
import Badge from "react-bootstrap/Badge";
import Collapse from "react-bootstrap/Collapse";
import Card from "react-bootstrap/Card";
import InputGroup from "react-bootstrap/InputGroup";

import { FormInput, FormLabel, FormSwitch } from "components/common/Form";
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
import PopoverWithHoverWrapper from "../PopoverWithHoverWrapper";

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
            <Card.Body>
                <FormSwitch name={getName("isEnabled")} control={control}>
                    FTP
                </FormSwitch>
                <Collapse in={formValues.isEnabled} className="vstack gap-2 mt-2">
                    <div>
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
                                    <FormLabel className="d-flex align-items-center gap-1">
                                        Host
                                        <PopoverWithHoverWrapper
                                            message={
                                                <>
                                                    To specify the server protocol, prepend the host with protocol
                                                    identifier (ftp and ftps are supported). If no protocol is specified
                                                    the default one (<code>ftp://</code>) will be used. You can also
                                                    enter a complete URL e.g.{" "}
                                                    <code>ftp://host.name:port/backup-folder/nested-backup-folder</code>
                                                </>
                                            }
                                        >
                                            <Icon icon="info" color="info" margin="m-0" />
                                        </PopoverWithHoverWrapper>
                                        {asyncTest.result?.Success ? (
                                            <Badge bg="success" pill>
                                                <Icon icon="check" />
                                                Successfully connected
                                            </Badge>
                                        ) : asyncTest.result?.Error ? (
                                            <Badge bg="danger" pill>
                                                <Icon icon="warning" />
                                                Failed connection
                                            </Badge>
                                        ) : null}
                                    </FormLabel>
                                    <FormInput
                                        name={getName("url")}
                                        control={control}
                                        placeholder="Enter a host"
                                        type="text"
                                        autoComplete="off"
                                    />
                                </div>
                                <div className="mb-2">
                                    <FormLabel>Username</FormLabel>
                                    <FormInput
                                        name={getName("userName")}
                                        control={control}
                                        placeholder="Enter a username"
                                        type="text"
                                        autoComplete="off"
                                    />
                                </div>
                                <div className="mb-2">
                                    <FormLabel>Password</FormLabel>
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
                                        <FormLabel>Certificate</FormLabel>
                                        <input id="filePicker" type="file" onChange={selectFile} className="d-none" />
                                        <InputGroup>
                                            <span className="static-name form-control d-flex align-items-center">
                                                {formValues.certificateAsBase64 ? "<certificate>" : "Select file..."}
                                            </span>
                                            <InputGroup.Text>
                                                <label htmlFor="filePicker" className="cursor-pointer">
                                                    <Icon icon="document" />
                                                    <span>Browse</span>
                                                </label>
                                            </InputGroup.Text>
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
                                        variant="secondary"
                                        onClick={asyncTest.execute}
                                        isSpinning={asyncTest.loading}
                                        icon="rocket"
                                    >
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
                    </div>
                </Collapse>
            </Card.Body>
        </Card>
    );
}

const fieldBase = "destinations.ftp";

type FormFieldNames = keyof FormDestinations["destinations"]["ftp"];

function getName(fieldName: FormFieldNames): `${typeof fieldBase}.${FormFieldNames}` {
    return `${fieldBase}.${fieldName}`;
}
