import useBoolean from "hooks/useBoolean";
import Collapse from "react-bootstrap/Collapse";
import Form from "react-bootstrap/Form";
import Row from "react-bootstrap/Row";
import { CloseButton, Col, Label, Modal, ModalBody, ModalFooter } from "reactstrap";
import { Icon } from "components/common/Icon";
import { Checkbox, Switch } from "components/common/Checkbox";
import React from "react";
import Button from "react-bootstrap/Button";

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
            contentClassName="modal-border bulge-primary"
        >
            <ModalBody className="vstack gap-4 position-relative">
                <div className="text-center">
                    <Icon icon="support" color="primary" className="fs-1" margin="m-0" />
                </div>

                <div className="position-absolute m-2 end-0 top-0">
                    <CloseButton onClick={toggle} />
                </div>
                <div className="text-center lead">Request support</div>

                <Form className="vstack gap-2">
                    <Form.Group>
                        <Label for="contactEmail">Contact email</Label>
                        <Form.Control
                            type="email"
                            name="contactEmail"
                            value="defaultEmailAssignedToLicense@client.com"
                            placeholder="Email"
                        />
                    </Form.Group>
                    <Row>
                        <Col>
                            <Form.Group>
                                <Label for="supportId">Support ID</Label>
                                <Form.Control type="number" name="supportId" value={supportId} disabled />
                            </Form.Group>
                        </Col>
                        <Col>
                            <Form.Group>
                                <Label for="LicenseId">License ID</Label>
                                <Form.Control type="text" name="supportId" value={licenseId} disabled />
                            </Form.Group>
                        </Col>
                    </Row>
                    <Form.Group>
                        <Label for="messageText">
                            Message <span className="text-muted">(optional)</span>
                        </Label>
                        <Form.Control as="textarea" name="text" id="messageText" rows={10} />
                    </Form.Group>
                    <div className="well p-3 rounded-2">
                        <Checkbox size="lg" selected={includeDebugPackage} toggleSelection={toggleIncludeDebugPackage}>
                            Include debug package
                        </Checkbox>
                        <Collapse in={includeDebugPackage}>
                            <div className="py-2">
                                <Switch selected={includeAllDatabases} toggleSelection={toggleIncludeAllDatabases}>
                                    Include all databases
                                </Switch>
                                <Collapse in={!includeAllDatabases}>
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
                <Button variant="outline-secondary" onClick={toggle} className="rounded-pill px-3">
                    Close
                </Button>
                <Button variant="primary" className="rounded-pill px-3">
                    <Icon icon="support" />
                    Request support
                </Button>
            </ModalFooter>
        </Modal>
    );
}
