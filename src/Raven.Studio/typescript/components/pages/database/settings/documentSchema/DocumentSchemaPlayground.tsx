import Col from "react-bootstrap/Col";
import Row from "react-bootstrap/Row";
import { AboutViewHeading } from "components/common/AboutView";
import Button from "react-bootstrap/Button";
import { Icon } from "components/common/Icon";
import PopoverWithHoverWrapper from "components/common/PopoverWithHoverWrapper";
import { HrHeader } from "components/common/HrHeader";
import React, { useRef } from "react";
import {
    RichPanel,
    RichPanelActions,
    RichPanelDetails,
    RichPanelHeader,
    RichPanelInfo,
    RichPanelName,
} from "components/common/RichPanel";
import { FormGroup, FormLabel } from "components/common/Form";
import Select from "components/common/select/Select";
import AceEditor from "components/common/ace/AceEditor";
import ReactAce from "react-ace/lib/ace";

export default function DocumentSchemaPlayground() {
    const aceRef = useRef<ReactAce>(null);

    return (
        <div className="content-margin">
            <Col>
                <Row>
                    <Col>
                        <AboutViewHeading marginBottom={4} title="Document Schema Playground" icon="rocket" />
                        <span>
                            Quickly create and test schemas against your documents without affecting your saved data.
                            The Schema Playground is a temporary workspace designed for safe experimentation.
                        </span>

                        <div className="mt-5 d-flex align-items-center justify-content-between">
                            <Button variant="secondary">
                                <Icon icon="close" />
                                Cancel
                            </Button>
                            <Button className="rounded-pill">
                                <Icon icon="rocket" />
                                Run test
                            </Button>
                        </div>

                        <div className="mt-4">
                            <HrHeader
                                count={3}
                                right={
                                    <Button size="xs" variant="info" className="rounded-pill">
                                        <Icon icon="plus" />
                                        Add new
                                    </Button>
                                }
                            >
                                <Icon icon="documents" />
                                <span>Collection specific document schemas</span>
                                <PopoverWithHoverWrapper message="info">
                                    <Icon icon="info" color="info" margin="ms-1" />
                                </PopoverWithHoverWrapper>
                            </HrHeader>

                            <RichPanel>
                                <RichPanelHeader>
                                    <RichPanelInfo>
                                        <RichPanelName>Document schema</RichPanelName>
                                    </RichPanelInfo>
                                    <RichPanelActions>
                                        <Button variant="danger">
                                            <Icon margin="m-0" icon="trash" />
                                        </Button>
                                    </RichPanelActions>
                                </RichPanelHeader>

                                <RichPanelDetails>
                                    <FormGroup className="w-100 mt-2">
                                        <FormGroup>
                                            <FormLabel>Collection</FormLabel>
                                            <Select
                                                placeholder="Select a collection (or enter a new one)"
                                                options={[
                                                    {
                                                        label: "Orders",
                                                        value: "orders",
                                                    },
                                                ]}
                                            />
                                        </FormGroup>
                                        <FormLabel>
                                            Document schema <Icon icon="info" color="info" margin="m-0" />
                                        </FormLabel>
                                        <AceEditor
                                            height="500px"
                                            aceRef={aceRef}
                                            isFullScreenLabelHidden
                                            actions={[
                                                { component: <AceEditor.FullScreenAction /> },
                                                { component: <AceEditor.FormatAction /> },
                                                { component: <AceEditor.ToggleNewLinesAction /> },
                                            ]}
                                            mode="json"
                                        />
                                    </FormGroup>
                                </RichPanelDetails>
                            </RichPanel>
                        </div>
                    </Col>
                </Row>
            </Col>
        </div>
    );
}
