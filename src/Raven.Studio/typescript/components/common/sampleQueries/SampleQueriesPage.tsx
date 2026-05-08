import React, { ReactNode, useState } from "react";
import Button from "react-bootstrap/Button";
import Tab from "react-bootstrap/Tab";
import Nav from "react-bootstrap/Nav";
import AceEditor from "components/common/ace/AceEditor";
import SampleScriptsList from "./partials/SampleScriptsList";
import MethodsTable from "./partials/MethodsTable";
import Card from "react-bootstrap/Card";
import { AboutViewHeading } from "components/common/AboutView";
import { FlexGrow } from "components/common/FlexGrow";
import { Icon } from "components/common/Icon";
import { MethodGroup, SampleScript } from "./partials/sampleQueriesTypes";
import "./SampleQueriesPage.scss";
import Row from "react-bootstrap/Row";
import Col from "react-bootstrap/Col";
import IconName from "../../../../typings/server/icons";
import SizeGetter from "components/common/SizeGetter";

type ActiveTab = "scripts" | "methods";

interface SampleQueriesPageProps {
    title: string;
    icon?: IconName;
    scripts: SampleScript[];
    methodGroups: MethodGroup[];
    onClose: () => void;
    initialScript?: string;
    aboutView?: ReactNode;
    onUpdateScript: (script: string) => void;
}

export default function SampleQueriesPage({
    title,
    icon,
    scripts,
    methodGroups,
    onClose,
    initialScript = "",
    aboutView,
    onUpdateScript,
}: SampleQueriesPageProps) {
    const [script, setScript] = useState(initialScript);
    const [activeTab, setActiveTab] = useState<ActiveTab>("scripts");

    const handleReset = () => {
        setScript(initialScript);
    };

    return (
        <div className="bs5 content-padding h-100 vstack gap-3 sample-queries-page">
            <div className="hstack gap-2 align-items-center mb-4">
                <Icon
                    onClick={onClose}
                    icon="arrow-thin-left"
                    size="lg"
                    margin="me-1"
                    className="hover-filter link-muted cursor-pointer"
                />
                <AboutViewHeading marginBottom={0} title={title} icon={icon} />
                <FlexGrow />
                {aboutView}
            </div>

            <div className="d-flex gap-2 mb-3">
                <Button variant="primary" onClick={() => onUpdateScript(script)}>
                    <Icon icon="save" />
                    Update script
                </Button>
                <Button variant="secondary" onClick={handleReset}>
                    <Icon icon="reset" />
                    Reset
                </Button>
            </div>
            <Row className="d-flex flex-row flex-grow-1 overflow-hidden">
                <Col xs={6} className="d-flex flex-column overflow-hidden">
                    <SizeGetter
                        isHeighRequired
                        render={({ height }) => (
                            <AceEditor mode="rql" value={script} onChange={setScript} height={`${height}px`} />
                        )}
                    />
                </Col>

                <Col xs={6} className="d-flex flex-column overflow-hidden h-100">
                    <Card className="vstack panel-bg-1 border border-color-light border-1 rounded flex-grow-1 overflow-hidden">
                        <Tab.Container
                            mountOnEnter
                            unmountOnExit
                            id="sample-queries-tabs"
                            activeKey={activeTab}
                            onSelect={(tab) => setActiveTab(tab as ActiveTab)}
                        >
                            <Nav variant="pills" className="gap-1 panel-bg-2 p-2 flex-shrink-0">
                                <Nav.Item>
                                    <Nav.Link eventKey="scripts">
                                        <Icon icon="document" />
                                        Sample scripts
                                    </Nav.Link>
                                </Nav.Item>
                                <Nav.Item>
                                    <Nav.Link eventKey="methods">
                                        <Icon icon="indent" />
                                        Methods
                                    </Nav.Link>
                                </Nav.Item>
                            </Nav>
                            <div className="flex-grow-1 overflow-y-auto">
                                <Tab.Content>
                                    <Tab.Pane eventKey="scripts">
                                        <SampleScriptsList scripts={scripts} onSelect={setScript} />
                                    </Tab.Pane>
                                    <Tab.Pane eventKey="methods">
                                        <MethodsTable methodGroups={methodGroups} />
                                    </Tab.Pane>
                                </Tab.Content>
                            </div>
                        </Tab.Container>
                    </Card>
                </Col>
            </Row>
        </div>
    );
}
