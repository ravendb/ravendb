import React, { useRef, useState } from "react";
import Tab from "react-bootstrap/Tab";
import Nav from "react-bootstrap/Nav";
import Card from "react-bootstrap/Card";
import { Icon } from "components/common/Icon";
import SampleScriptsList from "./partials/SampleScriptsList";
import MethodsTable from "./partials/MethodsTable";
import { MethodGroup, SampleScript } from "./partials/sampleQueriesTypes";
import "./SampleQueries.scss";

type ActiveTab = "scripts" | "methods";

export interface SampleQueriesTabsProps {
    scripts: SampleScript[];
    methodGroups: MethodGroup[];
    onSelect: (script: string) => void;
}

export default function SampleQueriesTabs({ scripts, methodGroups, onSelect }: SampleQueriesTabsProps) {
    const [activeTab, setActiveTab] = useState<ActiveTab>("scripts");
    const [methodSearch, setMethodSearch] = useState("");
    const tabContentRef = useRef<HTMLDivElement>(null);

    const handleTabSelect = (tab: ActiveTab) => {
        setActiveTab(tab);
        if (tabContentRef.current) {
            tabContentRef.current.scrollTop = 0;
        }
    };

    return (
        <Card className="panel-bg-1 border border-color-light sample-queries-tabs">
            <Tab.Container
                mountOnEnter
                unmountOnExit
                id="sample-queries-tabs"
                activeKey={activeTab}
                onSelect={(tab) => handleTabSelect(tab as ActiveTab)}
            >
                <Nav variant="pills" className="gap-1 panel-bg-2 sample-queries-nav">
                    <Nav.Item>
                        <Nav.Link eventKey="scripts" className="no-decor">
                            <Icon icon="document" />
                            Sample scripts
                        </Nav.Link>
                    </Nav.Item>
                    <Nav.Item>
                        <Nav.Link eventKey="methods" className="no-decor">
                            <Icon icon="indent" />
                            Methods
                        </Nav.Link>
                    </Nav.Item>
                </Nav>
                <Tab.Content ref={tabContentRef}>
                    <Tab.Pane eventKey="scripts">
                        <SampleScriptsList scripts={scripts} onSelect={onSelect} />
                    </Tab.Pane>
                    <Tab.Pane eventKey="methods">
                        <MethodsTable
                            methodGroups={methodGroups}
                            search={methodSearch}
                            onSearchChange={setMethodSearch}
                            onSelect={onSelect}
                        />
                    </Tab.Pane>
                </Tab.Content>
            </Tab.Container>
        </Card>
    );
}
