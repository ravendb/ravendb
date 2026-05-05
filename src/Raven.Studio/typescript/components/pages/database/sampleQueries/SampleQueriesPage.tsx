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
import { SampleScript, MethodGroup } from "./partials/sampleQueriesTypes";

type ActiveTab = "scripts" | "methods";

interface SampleQueriesPageProps {
    title: ReactNode;
    scripts: SampleScript[];
    methodGroups: MethodGroup[];
    backUrl: string;
    initialScript?: string;
    aboutView?: ReactNode;
    onUpdateScript: (script: string) => void;
}

export default function SampleQueriesPage({
    title,
    scripts,
    methodGroups,
    backUrl,
    initialScript = "",
    aboutView,
    onUpdateScript,
}: SampleQueriesPageProps) {
    const [script, setScript] = useState(initialScript);
    const [activeTab, setActiveTab] = useState<ActiveTab>("scripts");

    const handleReset = () => {
        setScript(initialScript);
    };

    const handleSelectSample = (sampleScript: string) => {
        setScript(sampleScript);
    };

    return (
        <div className="content-padding h-100 vstack gap-3">
            <div className="flex-shrink-0 hstack gap-2 align-items-start">
                <AboutViewHeading title={title} backUrl={backUrl} />
                <FlexGrow />
                {aboutView}
            </div>

            <div className="d-flex gap-2 mb-2">
                <Button variant="primary" onClick={() => onUpdateScript(script)}>
                    <Icon icon="save" />
                    Update script
                </Button>
                <Button variant="secondary" onClick={handleReset}>
                    <Icon icon="reset" />
                    Reset
                </Button>
            </div>
            <div className="d-flex flex-row gap-3 flex-grow-1 overflow-hidden">
                <div className="flex-grow-1 overflow-y-auto" style={{ minWidth: 0 }}>
                    <AceEditor mode="rql" value={script} onChange={setScript} minHeight={300} />
                </div>

                <div className="vstack overflow-hidden flex-shrink-0" style={{ width: "480px" }}>
                    <Card className="vstack panel-bg-1 border border-color-light border-1 rounded h-100 overflow-hidden">
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
                                        <SampleScriptsList scripts={scripts} onSelect={handleSelectSample} />
                                    </Tab.Pane>
                                    <Tab.Pane eventKey="methods">
                                        <MethodsTable methodGroups={methodGroups} />
                                    </Tab.Pane>
                                </Tab.Content>
                            </div>
                        </Tab.Container>
                    </Card>
                </div>
            </div>
        </div>
    );
}
