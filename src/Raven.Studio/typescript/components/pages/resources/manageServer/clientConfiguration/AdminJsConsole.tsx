import React from "react";
import {
    Form,
    Col,
    Button,
    Card,
    Row,
    Spinner,
    InputGroup,
    InputGroupText,
    Alert,
    CardHeader,
    CardBody,
    DropdownMenu,
    DropdownToggle,
    DropdownItem,
    UncontrolledDropdown,
} from "reactstrap";

import { LoadingView } from "components/common/LoadingView";
import { LoadError } from "components/common/LoadError";
import ClientConfigurationUtils from "components/common/clientConfiguration/ClientConfigurationUtils";
import useClientConfigurationFormController from "components/common/clientConfiguration/useClientConfigurationFormController";
import { tryHandleSubmit } from "components/utils/common";
import { Icon } from "components/common/Icon";
import { PopoverWithHover } from "components/common/PopoverWithHover";
import useClientConfigurationPopovers from "components/common/clientConfiguration/useClientConfigurationPopovers";

export default function AdminJSConsole() {
    // if (asyncGetGlobalClientConfiguration.loading) {
    //     return <LoadingView />;
    // }

    // if (asyncGetGlobalClientConfiguration.error) {
    //     return <LoadError error="Unable to load client global configuration" refresh={onRefresh} />;
    // }

    return (
        <div className="content-margin">
            <h2>
                <Icon icon="administrator-js-console" /> Admin JS Console
            </h2>
            <Alert color="warning">
                <Icon icon="warning" /> WARNING: Do not use the console unless you are sure about what you're doing.
                Running a script in the Admin Console could cause your server to crash, cause loss of data, or other
                irreversible harm.
            </Alert>
            <Card>
                <CardHeader className="hstack gap-4">
                    <h3 className="m-0">Script target</h3>
                    <UncontrolledDropdown>
                        <DropdownToggle caret>Server</DropdownToggle>
                        <DropdownMenu>
                            <DropdownItem>
                                <Icon icon="server" /> Server
                            </DropdownItem>
                            <DropdownItem divider />
                            <DropdownItem>
                                <Icon icon="database" />
                                DbName 1
                            </DropdownItem>
                            <DropdownItem>
                                <Icon icon="database" />
                                DbName 2
                            </DropdownItem>
                            <DropdownItem>
                                <Icon icon="database" />
                                DbName 3
                            </DropdownItem>
                        </DropdownMenu>
                    </UncontrolledDropdown>
                </CardHeader>
                <CardBody>
                    <h3>Script</h3>
                    <h3>Script result</h3>
                </CardBody>
            </Card>
        </div>
    );
}
