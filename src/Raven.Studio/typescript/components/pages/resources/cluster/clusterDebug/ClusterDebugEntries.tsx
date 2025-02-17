import React, { useState } from "react";
import { Nav, NavItem, NavLink, TabContent, TabPane } from "reactstrap";
import { Icon } from "components/common/Icon";
import { Badge } from "reactstrap";
import classNames from "classnames";
import { nodeData } from "./ClusterDebugSummary";
import { ClusterDebugEntry, entriesData } from "components/pages/resources/cluster/clusterDebug/ClusterDebugEntry";
import "./ClusterDebugEntries.scss";
import useBoolean from "hooks/useBoolean";
import ClusterDebugPagination from "components/pages/resources/cluster/clusterDebug/ClusterDebugPagination";

const showNodeIcon = (type: "Leader" | "Member") => (
    <Icon icon={type === "Leader" ? "node-leader" : "cluster-member"} color="node" />
);

export function ClusterDebugEntries() {
    const [activeTab, setActiveTab] = useState<string>(nodeData[0].name);

    const { value: panelCollapsed, toggle: togglePanelCollapsed } = useBoolean(true);

    const toggle = (tab: string) => {
        if (activeTab !== tab) setActiveTab(tab);
    };

    return (
        <div className="cluster-debug-entries">
            <Nav tabs>
                {nodeData.map((node) => (
                    <NavItem key={node.name}>
                        <NavLink
                            className={classNames({ active: activeTab === node.name }, "no-decor")}
                            onClick={() => {
                                toggle(node.name);
                            }}
                        >
                            <div className="d-flex gap-1 align-items-center">
                                <span>
                                    {showNodeIcon(node.type)}
                                    <span className="text-nowrap">{node.name}</span>
                                </span>
                                {node.current && (
                                    <Badge color="node" pill>
                                        Current
                                    </Badge>
                                )}
                            </div>
                        </NavLink>
                    </NavItem>
                ))}
            </Nav>
            <TabContent activeTab={activeTab}>
                {nodeData.map((node) => (
                    <TabPane key={node.name} tabId={node.name}>
                        {entriesData.map((entry, index) => (
                            <ClusterDebugEntry
                                key={index}
                                entry={entry}
                                panelCollapsed={panelCollapsed}
                                togglePanelCollapsed={togglePanelCollapsed}
                            />
                        ))}
                    </TabPane>
                ))}
            </TabContent>
            <ClusterDebugPagination />
        </div>
    );
}

export default ClusterDebugEntries;
