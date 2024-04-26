import React from "react";
import { Col, Row } from "reactstrap";
import { AboutViewAnchored, AboutViewHeading, AccordionItemWrapper } from "components/common/AboutView";
import FilterIndexErrors from "components/pages/database/indexes/errors/FilterIndexErrors";
import { IndexErrorsPanel } from "components/pages/database/indexes/errors/IndexErrorsPanel";

interface IndexErrorProps {}

export function IndexErrors(props: IndexErrorProps) {
    return (
        <div className="content-margin">
            <Col xxl={12}>
                <Row className="gy-sm">
                    <Col>
                        <div className="flex-shrink-0 hstack gap-2 align-items-start">
                            <AboutViewHeading icon="index-errors" title="Index Errors" />
                        </div>
                        <FilterIndexErrors />
                        <IndexErrorsPanel hasErrors={true} />
                        <IndexErrorsPanel hasErrors={false} />
                    </Col>
                    <Col sm={12} lg={4}>
                        <AboutViewAnchored>
                            <AccordionItemWrapper
                                targetId="about"
                                icon="about"
                                color="info"
                                heading="About this view"
                                description="Get additional info on this feature"
                            >
                                <p>
                                    Maintaining multiple indexes can lower performance. Every time data is inserted,
                                    updated, or deleted, the corresponding indexes need to be updated as well, which can
                                    lead to increased write latency.
                                </p>
                                <p className="mb-0">
                                    To counter these performance issues, RavenDB recommends a set of actions to optimize
                                    the number of indexes. Note that you need to update the index reference in your
                                    application.
                                </p>
                            </AccordionItemWrapper>
                        </AboutViewAnchored>
                    </Col>
                </Row>
            </Col>
        </div>
    );
}
