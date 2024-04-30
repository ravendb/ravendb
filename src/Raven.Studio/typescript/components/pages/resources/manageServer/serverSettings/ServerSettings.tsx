import React from "react";
import { Col, Input, Row } from "reactstrap";
import { AboutViewHeading } from "components/common/AboutView";
import { todo } from "common/developmentHelper";
import { ServerSettingsVirtualGrid } from "components/pages/resources/manageServer/serverSettings/ServerSettingsVirtualGrid";
import ButtonWithSpinner from "components/common/ButtonWithSpinner";
import { Icon } from "components/common/Icon";
import { ServerSettingsInfoHub } from "components/pages/resources/manageServer/serverSettings/ServerSettingsInfoHub";

interface ServerSettingsProps {}

export function ServerSettings(props: ServerSettingsProps) {
    todo("Feature", "Damian", "Add logic");
    todo("Feature", "Damian", "Connect to studio");
    todo("Feature", "Damian", "Remove old code");
    todo("Other", "Danielle", "Fill Info Hub");
    return (
        <div className="content-margin">
            <Col xxl={12}>
                <Row className="gy-sm">
                    <Col>
                        <AboutViewHeading icon="server-settings" title="Server Settings" />
                        <div className="d-flex justify-content-between align-items-end flex-wrap gap-3 my-3">
                            <div className="flex-grow-1">
                                <div className="small-label ms-1 mb-1">Filter by configuration key</div>
                                <div className="clearable-input">
                                    <Input
                                        type="text"
                                        accessKey="/"
                                        placeholder="e.g. Cluster.Tcp"
                                        title="Filter server settings"
                                        className="filtering-input rounded-pill"
                                        style={{ maxWidth: "240px" }}
                                        // value={filter.searchText}
                                        // onChange={(e) => onFilterValueChange("searchText", e.target.value)}
                                    />
                                    {/*{filter.searchText && (*/}
                                    {/*    <div className="clear-button">*/}
                                    {/*        <Button color="secondary" size="sm"*/}
                                    {/*                onClick={() => onFilterValueChange("searchText", "")}>*/}
                                    {/*            <Icon icon="clear" margin="m-0"/>*/}
                                    {/*        </Button>*/}
                                    {/*    </div>*/}
                                    {/*)}*/}
                                </div>
                            </div>
                            <ButtonWithSpinner isSpinning={false}>
                                <Icon icon="refresh" /> Refresh
                            </ButtonWithSpinner>
                        </div>
                        <div className="position-relative" style={{ height: "640px" }}>
                            <ServerSettingsVirtualGrid />
                        </div>
                    </Col>
                    <Col sm={12} lg={4}>
                        <ServerSettingsInfoHub />
                    </Col>
                </Row>
            </Col>
        </div>
    );
}
