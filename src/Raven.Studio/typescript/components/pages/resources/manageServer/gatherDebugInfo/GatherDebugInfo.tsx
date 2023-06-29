import { Icon } from "components/common/Icon";
import React, { useState } from "react";
import { Button, Card, CardBody, Col, Input, InputGroup } from "reactstrap";
import IconName from "typings/server/icons";
import "./GatherDebugInfo.scss";
import { Checkbox, Switch } from "components/common/Checkbox";
import useBoolean from "components/hooks/useBoolean";

const infoPackageImg = require("Content/img/info_package.svg");
const createPackageImg = require("Content/img/create_package.svg");

function GatherDebugInfo() {
    const { value: selected, toggle } = useBoolean(true);

    const [serverDataTypeSelection, setServerDataType] = useState(true);
    const toggleServerDataTypeSelection = () => {
        setServerDataType(!serverDataTypeSelection);
    };

    const [databasesDataTypeSelection, setDatabasesDataType] = useState(true);
    const toggleDatabasesDataTypeSelection = () => {
        setDatabasesDataType(!databasesDataTypeSelection);
    };

    const [logsDataTypeSelection, setLogsDataType] = useState(true);
    const toggleLogsDataTypeSelection = () => {
        setLogsDataType(!logsDataTypeSelection);
    };

    const [allDatabasesSelection, setAllDatabases] = useState(true);
    const toggleAllDatabasesSelection = () => {
        setAllDatabases(!allDatabasesSelection);
    };

    return (
        <Col lg="6" md="9" sm="12" className="gather-debug-info">
            <Card>
                <CardBody className="d-flex flex-center flex-column">
                    <img src={infoPackageImg} alt="Info Package" width="120" />
                    <h3 className="mt-3">Create Debug Package</h3>
                    <p className="lead text-center w-75 fs-5">
                        Generate a comprehensive diagnostic package to assist in troubleshooting and resolving issues.
                    </p>
                    <IconList />
                    <div className="position-relative d-flex flex-row gap-4 w-100 flex-wrap">
                        <div className="d-flex flex-column half-width-section">
                            <h4>Select data source</h4>
                            <div className="d-flex flex-column well px-4 py-3 border-radius-xs">
                                <Checkbox
                                    selected={serverDataTypeSelection}
                                    toggleSelection={toggleServerDataTypeSelection}
                                >
                                    Server
                                </Checkbox>
                                <Checkbox
                                    selected={databasesDataTypeSelection}
                                    toggleSelection={toggleDatabasesDataTypeSelection}
                                >
                                    Databases
                                </Checkbox>
                                <Checkbox
                                    selected={logsDataTypeSelection}
                                    toggleSelection={toggleLogsDataTypeSelection}
                                >
                                    Logs
                                </Checkbox>
                            </div>
                            <h4 className="mt-3 d-flex justify-content-between align-items-center">
                                Select databases
                                <Switch
                                    selected={allDatabasesSelection}
                                    toggleSelection={toggleAllDatabasesSelection}
                                    color="primary"
                                >
                                    {" "}
                                    <small>Select all</small>
                                </Switch>
                            </h4>
                            {!allDatabasesSelection && (
                                <div className="d-flex flex-column well px-4 py-3 border-radius-xs">
                                    <Checkbox selected={selected} toggleSelection={toggle}>
                                        DemoUser-3fdd8187-6940-4e25-a362-d57533
                                    </Checkbox>
                                </div>
                            )}
                        </div>
                        <div className="d-flex flex-column half-width-section">
                            <div className="position-sticky package-download-section d-flex flex-column align-items-center well border-radius-xs p-4 gap-4">
                                <img src={createPackageImg} alt="Info Package" width="90" />
                                <h4 className="m-0">Create package for</h4>
                                <InputGroup className="d-flex flex-column align-items-center gap-4">
                                    <Input
                                        id="packageDestinationSelect"
                                        name="select"
                                        type="select"
                                        className="w-100 rounded-pill"
                                    >
                                        <option value="" hidden>
                                            Select...
                                        </option>
                                        <option value="cluster">Entire cluster</option>
                                        <option value="server">Current server only</option>
                                    </Input>
                                    <Button color="primary" className="rounded-pill" disabled>
                                        <Icon icon="default" />
                                        Download
                                    </Button>
                                </InputGroup>
                            </div>
                        </div>
                    </div>
                </CardBody>
            </Card>
        </Col>
    );
}

function IconList() {
    const icons: IconName[] = ["replication", "stats", "io-test", "storage", "memory", "other"];
    const labels = ["Replication", "Performance", "I/O", "Storage", "Memory", "Other"];
    return (
        <div className="d-flex flex-row my-3 gap-4 flex-wrap justify-content-center icons-list">
            {icons.map((icon, index) => (
                <div key={icon} className="d-flex flex-column align-items-center text-center gap-3">
                    <Icon icon={icon} margin="m-0" />
                    <p>{labels[index]}</p>
                </div>
            ))}
        </div>
    );
}

export default GatherDebugInfo;
