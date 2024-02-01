import useBoolean from "hooks/useBoolean";
import { Button, Col, Collapse, Form, FormGroup, Input, Label, Modal, ModalBody, ModalFooter, Row } from "reactstrap";
import { Icon } from "components/common/Icon";
import { Checkbox, Switch } from "components/common/Checkbox";
import React from "react";

interface RequestSupportModalProps {
    visible: boolean;
    toggle: () => void;
    supportId: string;
    licenseId: string;
}

export function RequestSupportModal(props: RequestSupportModalProps) {
    const { visible, toggle, supportId, licenseId } = props;

    const { value: includeDebugPackage, toggle: toggleIncludeDebugPackage } = useBoolean(false);
    const { value: includeAllDatabases, toggle: toggleIncludeAllDatabases } = useBoolean(true);

    return (
        <Modal
            isOpen={visible}
            toggle={toggle}
            wrapClassName="bs5"
            centered
            size="lg"
            contentClassName={`modal-border bulge-primary`}
        >
            <ModalBody className="vstack gap-4 position-relative">
                <div className="text-center">
                    <Icon icon="support" color="primary" className="fs-1" margin="m-0" />
                </div>

                <div className="position-absolute m-2 end-0 top-0">
                    <Button close onClick={toggle} />
                </div>
                <div className="text-center lead">Request support</div>

                <Form className="vstack gap-2">
                    <FormGroup>
                        <Label for="contactEmail">Contact email</Label>
                        <Input
                            type="email"
                            name="contactEmail"
                            value="defaultEmailAssignedToLicense@client.com"
                            placeholder="Email"
                        />
                    </FormGroup>
                    <Row>
                        <Col>
                            <FormGroup>
                                <Label for="supportId">Support ID</Label>
                                <Input type="number" name="supportId" value={supportId} disabled />
                            </FormGroup>
                        </Col>
                        <Col>
                            <FormGroup>
                                <Label for="LicenseId">License ID</Label>
                                <Input type="text" name="supportId" value={licenseId} disabled />
                            </FormGroup>
                        </Col>
                    </Row>
                    <FormGroup>
                        <Label for="messageText">
                            Message <span className="text-muted">(optional)</span>
                        </Label>
                        <Input type="textarea" name="text" id="messageText" rows={10} />
                    </FormGroup>
                    <div className="well p-3 rounded-2">
                        <Checkbox size="lg" selected={includeDebugPackage} toggleSelection={toggleIncludeDebugPackage}>
                            Include debug package
                        </Checkbox>
                        <Collapse isOpen={includeDebugPackage}>
                            <div className="py-2">
                                <Switch selected={includeAllDatabases} toggleSelection={toggleIncludeAllDatabases}>
                                    Include all databases
                                </Switch>
                                <Collapse isOpen={!includeAllDatabases}>
                                    <div className="vstack">
                                        <Checkbox selected={null} toggleSelection={null}>
                                            Database1
                                        </Checkbox>
                                        <Checkbox selected={null} toggleSelection={null}>
                                            Database2
                                        </Checkbox>
                                        <Checkbox selected={null} toggleSelection={null}>
                                            Database3
                                        </Checkbox>
                                    </div>
                                </Collapse>
                                <div className="d-flex gap-4">
                                    <Checkbox selected={null} toggleSelection={null}>
                                        Server
                                    </Checkbox>
                                    <Checkbox selected={null} toggleSelection={null}>
                                        Databases
                                    </Checkbox>
                                    <Checkbox selected={null} toggleSelection={null}>
                                        Logs
                                    </Checkbox>
                                </div>
                            </div>
                        </Collapse>
                    </div>
                </Form>
            </ModalBody>
            <ModalFooter>
                <Button color="secondary" outline onClick={toggle} className="rounded-pill px-3">
                    Close
                </Button>
                <Button color="primary" className="rounded-pill px-3">
                    <Icon icon="support" />
                    Request support
                </Button>
            </ModalFooter>
        </Modal>
    );
}
