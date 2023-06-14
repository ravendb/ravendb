import React, { useMemo } from "react";
import {
    Card,
    Alert,
    CardHeader,
    CardBody,
    DropdownMenu,
    DropdownToggle,
    DropdownItem,
    UncontrolledDropdown,
    Row,
    Col,
    Button,
} from "reactstrap";
import { Icon } from "components/common/Icon";
import AboutView from "components/common/AboutView";
import "./AdminJsConsole.scss";
import database from "models/resources/database";
import rqlLanguageService from "common/rqlLanguageService";
import AceEditor from "components/common/AceEditor";

interface AdminJSConsoleProps {
    db: database;
}

export default function AdminJSConsole({ db }: AdminJSConsoleProps) {
    // TODO: remove after testing
    const languageService1 = new rqlLanguageService(db, ["from-react", "from-react2"], "Select");
    const languageService2 = new rqlLanguageService(
        db,
        ko.observableArray(["from-knockout", "from-knockout-2"]),
        "Select"
    );

    return (
        <div className="content-margin">
            <Row>
                <Col xxl={9}>
                    <Row>
                        <Col>
                            <h2>
                                <Icon icon="administrator-js-console" /> Admin JS Console
                            </h2>
                        </Col>
                        <Col sm={"auto"}>
                            <AboutView>
                                <Row>
                                    <Col sm={"auto"}>
                                        <Icon
                                            className="fs-1"
                                            icon="administrator-js-console"
                                            color="info"
                                            margin="m-0"
                                        />
                                    </Col>
                                    <Col>
                                        <p>
                                            <strong>Admin JS Console</strong> is a specialized feature primarily
                                            intended for resolving server errors. It provides a direct interface to the
                                            underlying system, granting the capacity to execute scripts for intricate
                                            server operations.
                                        </p>
                                        <p>
                                            It is predominantly intended for advanced troubleshooting and rectification
                                            procedures executed by system administrators or RavenDB support.
                                        </p>
                                        <hr />
                                        <div className="small-label mb-2">useful links</div>
                                        <a href="https://ravendb.net/l/IBUJ7M/6.0/Csharp">
                                            <Icon icon="newtab" /> Docs - Admin JS Console
                                        </a>
                                    </Col>
                                </Row>
                            </AboutView>
                        </Col>
                    </Row>

                    <Alert color="warning hstack gap-4">
                        <div className="flex-shrink-0">
                            <Icon icon="warning" /> WARNING
                        </div>
                        <div>
                            Do not use the console unless you are sure about what you&apos;re doing. Running a script in
                            the Admin Console could cause your server to crash, cause loss of data, or other
                            irreversible harm.
                        </div>
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
                            <div className="text-info">
                                Accessible within the script under <code>server</code> variable
                            </div>
                        </CardHeader>
                        <CardBody>
                            <div className="admin-js-console-grid">
                                <div>
                                    <h3>Script</h3>
                                </div>
                                <AceEditor rqlLanguageService={languageService1} />
                                <div className="run-script-button">
                                    <Button color="primary" size="lg" className="px-4 py-2">
                                        <Icon icon="play" className="fs-1 d-inline-block" margin="mb-2" />
                                        <div className="kbd">
                                            <kbd>ctrl</kbd> <strong>+</strong> <kbd>enter</kbd>
                                        </div>
                                    </Button>
                                </div>
                                <div>
                                    <h3>Script result</h3>
                                </div>
                                <AceEditor rqlLanguageService={languageService2} />
                            </div>
                        </CardBody>
                    </Card>
                </Col>
            </Row>
        </div>
    );
}
