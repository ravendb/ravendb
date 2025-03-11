import useBoolean from "hooks/useBoolean";
import Collapse from "react-bootstrap/Collapse";
import Form from "react-bootstrap/Form";
import Row from "react-bootstrap/Row";
import Col from "react-bootstrap/Col";

import { Icon } from "components/common/Icon";
import { Checkbox, Switch } from "components/common/Checkbox";
import React from "react";
import Button from "react-bootstrap/Button";
import Modal from "components/common/Modal";
import { FormGroup } from "components/common/Form";

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
            show={visible}
            toggle={toggle}
            wrapClassName="bs5"
            centered
            size="lg"
            contentClassName="modal-border bulge-primary"
        >
            <Modal.Header className="vstack gap-4 " onCloseClick={toggle}>
                <div className="text-center">
                    <Icon icon="support" color="primary" className="fs-1" margin="m-0" />
                </div>

                <div className="text-center lead">Request support</div>
            </Modal.Header>
            <Modal.Body className="vstack">
                <Form className="vstack gap-2">
                    <FormGroup>
                        <Form.Label htmlFor="contactEmail">Contact email</Form.Label>
                        <Form.Control
                            type="email"
                            name="contactEmail"
                            value="defaultEmailAssignedToLicense@client.com"
                            placeholder="Email"
                        />
                    </FormGroup>
                    <Row>
                        <Col>
                            <FormGroup>
                                <Form.Label htmlFor="supportId">Support ID</Form.Label>
                                <Form.Control type="number" name="supportId" value={supportId} disabled />
                            </FormGroup>
                        </Col>
                        <Col>
                            <FormGroup>
                                <Form.Label htmlFor="LicenseId">License ID</Form.Label>
                                <Form.Control type="text" name="supportId" value={licenseId} disabled />
                            </FormGroup>
                        </Col>
                    </Row>
                    <FormGroup>
                        <Form.Label htmlFor="messageText">
                            Message <span className="text-muted">(optional)</span>
                        </Form.Label>
                        <Form.Control as="textarea" name="text" id="messageText" rows={10} />
                    </FormGroup>
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
            </Modal.Body>
            <Modal.Footer>
                <Button variant="outline-secondary" onClick={toggle} className="rounded-pill px-3">
                    Close
                </Button>
                <Button variant="primary" className="rounded-pill px-3">
                    <Icon icon="support" />
                    Request support
                </Button>
            </Modal.Footer>
        </Modal>
    );
}
