import Col from "react-bootstrap/Col";
import Row from "react-bootstrap/Row";
import { AboutViewHeading } from "components/common/AboutView";
import { Checkbox } from "components/common/Checkbox";
import React, { useRef } from "react";
import Button from "react-bootstrap/Button";
import { Icon } from "components/common/Icon";
import Select from "components/common/select/Select";
import { HrHeader } from "components/common/HrHeader";
import PopoverWithHoverWrapper from "components/common/PopoverWithHoverWrapper";
import {
    RichPanel,
    RichPanelActions,
    RichPanelDetails,
    RichPanelHeader,
    RichPanelInfo,
    RichPanelName,
    RichPanelSelect,
} from "components/common/RichPanel";
import { FormGroup, FormLabel } from "components/common/Form";
import AceEditor from "components/common/ace/AceEditor";
import ReactAce from "react-ace/lib/ace";
import useBoolean from "hooks/useBoolean";
import { useAppUrls } from "hooks/useAppUrls";
import { useAppSelector } from "components/store";
import { databaseSelectors } from "components/common/shell/databaseSliceSelectors";
import { ConditionalPopover } from "components/common/ConditionalPopover";

export default function DocumentSchema() {
    const databaseName = useAppSelector(databaseSelectors.activeDatabaseName);
    const aceRef = useRef<ReactAce>(null);
    const { appUrl } = useAppUrls();
    const { value: isEditingSchema, toggle: toggleEditingSchema } = useBoolean(false);
    return (
        <div className="content-margin">
            <Col>
                <Row>
                    <Col>
                        <AboutViewHeading marginBottom={4} title="Document Schema" icon="document" />

                        <div className="d-flex align-items-center justify-content-between">
                            <Checkbox
                                selected={false}
                                indeterminate={false}
                                toggleSelection={() => console.log("toggle selection")}
                                color="primary"
                                title="Select all or none"
                                size="lg"
                            >
                                <span className="small-label">Select All</span>
                            </Checkbox>

                            <a
                                href={appUrl.forDocumentSchemaPlayground(databaseName)}
                                className="btn btn-secondary rounded-pill"
                            >
                                <Icon icon="rocket" />
                                Schema playground
                            </a>
                        </div>

                        <div className="mt-3">
                            <span className="small-label">Multi</span>
                            <Select options={options} isMulti value={options} />
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
                                        <RichPanelSelect>
                                            <Checkbox toggleSelection={() => {}} selected={false} />
                                        </RichPanelSelect>
                                        <RichPanelName>Orders</RichPanelName>
                                    </RichPanelInfo>
                                    <RichPanelActions>
                                        <ConditionalPopover
                                            conditions={{
                                                isActive: isEditingSchema,
                                                message:
                                                    " The schema must be saved in order to test it against existing documents.",
                                            }}
                                        >
                                            <Button variant="secondary" disabled={isEditingSchema}>
                                                <Icon margin="m-0" icon="rocket" />
                                            </Button>
                                        </ConditionalPopover>

                                        <Button
                                            onClick={toggleEditingSchema}
                                            variant={isEditingSchema ? "success" : "secondary"}
                                        >
                                            <Icon margin="m-0" icon={isEditingSchema ? "save" : "edit"} />
                                            {isEditingSchema && <span className="ms-1">Save</span>}
                                        </Button>
                                        {isEditingSchema ? (
                                            <Button variant="secondary">
                                                <Icon icon="close" />
                                                Discard
                                            </Button>
                                        ) : (
                                            <Button variant="danger">
                                                <Icon margin="m-0" icon="trash" />
                                            </Button>
                                        )}
                                    </RichPanelActions>
                                </RichPanelHeader>

                                {isEditingSchema && <RichPanelDetailsEditSchema aceRef={aceRef} />}
                                {!isEditingSchema && <RichPanelDetailsViewSchema aceRef={aceRef} />}
                            </RichPanel>
                        </div>
                    </Col>
                </Row>
            </Col>
        </div>
    );
}

interface RichPanelDetailsProps {
    aceRef: React.RefObject<ReactAce>;
}

const RichPanelDetailsViewSchema = ({ aceRef }: RichPanelDetailsProps) => {
    return (
        <RichPanelDetails>
            <FormGroup className="w-100 mt-2">
                <FormLabel>Document schema (Read only)</FormLabel>
                <AceEditor
                    height="300px"
                    aceRef={aceRef}
                    isFullScreenLabelHidden
                    actions={[
                        { component: <AceEditor.FullScreenAction /> },
                        { component: <AceEditor.FormatAction /> },
                        { component: <AceEditor.ToggleNewLinesAction /> },
                        {
                            component: <AceEditor.HelpAction message={<div>test</div>} />,
                            position: "bottom",
                        },
                    ]}
                    mode="json"
                    value={JSON.stringify(placeholderValue, null, 4)}
                />
            </FormGroup>
        </RichPanelDetails>
    );
};

const RichPanelDetailsEditSchema = ({ aceRef }: RichPanelDetailsProps) => {
    return (
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
                    height="900px"
                    aceRef={aceRef}
                    isFullScreenLabelHidden
                    actions={[
                        { component: <AceEditor.FullScreenAction /> },
                        { component: <AceEditor.FormatAction /> },
                        { component: <AceEditor.ToggleNewLinesAction /> },
                        {
                            component: <AceEditor.HelpAction message={<div>test</div>} />,
                            position: "bottom",
                        },
                    ]}
                    mode="json"
                    value={JSON.stringify(placeholderValue, null, 4)}
                />
            </FormGroup>
        </RichPanelDetails>
    );
};

const options = [
    { value: "chocolate", label: "Chocolate" },
    { value: "strawberry", label: "Strawberry" },
    { value: "vanilla", label: "Vanilla" },
];

const placeholderValue = {
    type: "object",
    properties: {
        name: {
            type: "string",
            description: "user name",
        },
        age: {
            type: "integer",
        },
        addresses: {
            type: "array",
            items: {
                $ref: "#/definitions/address",
            },
        },
        services: {
            anyOf: [
                {
                    $ref: "#/definitions/service1",
                },
                {
                    $ref: "#/definitions/service2",
                },
            ],
        },
    },
    required: ["name"],
};
