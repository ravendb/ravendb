import React, { useRef, useState } from "react";
import Tab from "react-bootstrap/Tab";
import Nav from "react-bootstrap/Nav";
import Card from "react-bootstrap/Card";
import Button from "react-bootstrap/Button";
import Form from "react-bootstrap/Form";
import { motion } from "motion/react";
import { Icon } from "components/common/Icon";
import useUniqueId from "components/hooks/useUniqueId";
import { SamplesTab } from "./partials/sampleQueriesTypes";
import "./SampleQueries.scss";

export interface SamplesTabsProps {
    tabs: SamplesTab[];
    onSelect: (script: string) => void;
    onClose?: () => void;
}

export default function SamplesTabs({ tabs, onSelect, onClose }: SamplesTabsProps) {
    const [activeTabKey, setActiveTabKey] = useState(tabs[0]?.key);
    const [searchByTab, setSearchByTab] = useState<Record<string, string>>({});
    const tabContentRef = useRef<HTMLDivElement>(null);
    const tabsId = useUniqueId("samples-tabs");

    const activeTab = tabs.find((tab) => tab.key === activeTabKey);
    const activeSearch = searchByTab[activeTabKey] ?? "";

    const handleTabSelect = (tabKey: string) => {
        setActiveTabKey(tabKey);
        if (tabContentRef.current) {
            tabContentRef.current.scrollTop = 0;
        }
    };

    const handleSearchChange = (value: string) => {
        setSearchByTab((prev) => ({ ...prev, [activeTabKey]: value }));
    };

    return (
        <Card className="panel-bg-1 border border-color-light sample-queries-tabs">
            <Tab.Container
                mountOnEnter
                unmountOnExit
                id={tabsId}
                activeKey={activeTabKey}
                onSelect={handleTabSelect}
            >
                <Nav variant="pills" className="gap-1 panel-bg-2 sample-queries-nav">
                    {tabs.map((tab) => (
                        <Nav.Item key={tab.key}>
                            <Nav.Link eventKey={tab.key} className="no-decor">
                                <Icon icon={tab.icon} />
                                {tab.label}
                            </Nav.Link>
                        </Nav.Item>
                    ))}
                    {onClose && (
                        <Button
                            variant="link"
                            size="sm"
                            className="ms-auto p-1 text-reset sample-queries-close"
                            title="Close"
                            onClick={onClose}
                        >
                            <motion.span
                                className="d-inline-flex"
                                initial={{ opacity: 0.6 }}
                                whileHover={{ opacity: 1 }}
                                whileTap={{ scale: 0.85 }}
                            >
                                <Icon icon="cancel" margin="m-0" />
                            </motion.span>
                        </Button>
                    )}
                </Nav>
                {activeTab?.hasSearch && (
                    <div className="methods-search-wrapper position-relative panel-bg-1 px-3">
                        <Icon icon="search" margin="m-0" className="methods-search-icon position-absolute" />
                        <Form.Control
                            placeholder={activeTab.searchPlaceholder ?? "Search"}
                            className="rounded-1 methods-search-input"
                            value={activeSearch}
                            onChange={(e) => handleSearchChange(e.target.value)}
                        />
                    </div>
                )}
                <Tab.Content ref={tabContentRef}>
                    {tabs.map((tab) => (
                        <Tab.Pane key={tab.key} eventKey={tab.key}>
                            {tab.content({ onSelect, search: searchByTab[tab.key] ?? "" })}
                        </Tab.Pane>
                    ))}
                </Tab.Content>
            </Tab.Container>
        </Card>
    );
}
