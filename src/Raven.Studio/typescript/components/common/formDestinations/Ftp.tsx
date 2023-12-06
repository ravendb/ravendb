import React from "react";
import { Button, Card, CardBody, Collapse, Label, PopoverBody, UncontrolledPopover } from "reactstrap";
import { FormInput, FormSwitch } from "components/common/Form";
import { useFormContext, useWatch } from "react-hook-form";
import { FlexGrow } from "components/common/FlexGrow";
import { Icon } from "components/common/Icon";
import { FormDestinations } from "./formDestinationsUtils";
import OverrideConfiguration from "./OverrideConfiguration";

export default function Ftp() {
    const { control } = useFormContext<FormDestinations>();
    const { ftp: formValues } = useWatch({ control });

    return (
        <Card className="well">
            <CardBody>
                <FormSwitch name="ftp.isEnabled" control={control}>
                    FTP
                </FormSwitch>
                <Collapse isOpen={formValues.isEnabled} className="mt-2">
                    <FormSwitch
                        name="ftp.isOverrideConfig"
                        control={control}
                        className="ms-3 mb-2 w-100"
                        color="secondary"
                    >
                        Override configuration via external script
                    </FormSwitch>
                    {formValues.isOverrideConfig ? (
                        <OverrideConfiguration formName="ftp" />
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
                                    name="ftp.url"
                                    control={control}
                                    placeholder="Enter a host"
                                    type="text"
                                    className="mb-2"
                                />
                            </div>
                            <div>
                                <Label className="mb-0 md-label">Username</Label>
                                <FormInput
                                    name="ftp.userName"
                                    control={control}
                                    placeholder="Enter a username"
                                    type="text"
                                    className="mb-2"
                                />
                            </div>
                            <div>
                                <Label className="mb-0 md-label">Password</Label>
                                <FormInput
                                    name="ftp.password"
                                    control={control}
                                    placeholder="Enter a password"
                                    type="password"
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
