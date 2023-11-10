import React, { useState } from "react";
import { Button, Card, CardBody, Collapse, Label, PopoverBody, UncontrolledPopover } from "reactstrap";
import { FormInput, FormSwitch } from "components/common/Form";
import { useForm } from "react-hook-form";
import OverrideConfiguration from "components/common/destinations/OverrideConfiguration";
import { FlexGrow } from "components/common/FlexGrow";
import { Icon } from "components/common/Icon";

interface FtpProps {
    className?: string;
}
const Ftp = (props: FtpProps) => {
    const { className } = props;
    const { control } = useForm<null>({});
    const [isOpen, setIsOpen] = useState(false);
    const [isOverrideConfigurationEnabled, setOverrideConfiguration] = useState(false);
    const toggle = () => setIsOpen(!isOpen);
    const toggleOverrideConfiguration = () => setOverrideConfiguration(!isOverrideConfigurationEnabled);
    return (
        <div className={className}>
            <Card className="well">
                <CardBody>
                    <FormSwitch name="ftp" control={control} onChange={toggle}>
                        FTP
                    </FormSwitch>
                    <Collapse isOpen={isOpen} className="mt-2">
                        <FormSwitch
                            name="overrideConfiguration"
                            control={control}
                            className="ms-3 mb-2 w-100"
                            onChange={toggleOverrideConfiguration}
                            color="secondary"
                        >
                            Override configuration via external script
                        </FormSwitch>
                        {!isOverrideConfigurationEnabled && (
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
                                            To specify the server protocol, prepend the host with protocol identifier
                                            (ftp and ftps are supported). If no protocol is specified the default one (
                                            <code>ftp://</code>) will be used. You can also enter a complete URL e.g.{" "}
                                            <code>ftp://host.name:port/backup-folder/nested-backup-folder</code>
                                        </PopoverBody>
                                    </UncontrolledPopover>
                                    <FormInput
                                        name="host"
                                        control={control}
                                        placeholder="Enter a host"
                                        type="text"
                                        className="mb-2"
                                    />
                                </div>
                                <div>
                                    <Label className="mb-0 md-label">Username</Label>
                                    <FormInput
                                        name="username"
                                        control={control}
                                        placeholder="Enter a username"
                                        type="text"
                                        className="mb-2"
                                    />
                                </div>
                                <div>
                                    <Label className="mb-0 md-label">Password</Label>
                                    <FormInput
                                        name="password"
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
                        {isOverrideConfigurationEnabled && <OverrideConfiguration />}
                    </Collapse>
                </CardBody>
            </Card>
        </div>
    );
};

export default Ftp;
