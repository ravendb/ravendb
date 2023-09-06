import React from "react";
import { Col, Row } from "reactstrap";
import { AboutViewAnchored, AboutViewHeading, AccordionItemWrapper } from "components/common/AboutView";
import { Icon } from "components/common/Icon";
import { HrHeader } from "components/common/HrHeader";
import { useServices } from "components/hooks/useServices";
import { useAsync } from "react-async-hook";
import AccordionLicenseLimited from "components/common/AccordionLicenseLimited";
import { todo } from "common/developmentHelper";
import { useAppUrls } from "components/hooks/useAppUrls";
import { useAppSelector } from "components/store";
import { licenseSelectors } from "components/common/shell/licenseSlice";
import { CounterBadge } from "components/common/CounterBadge";
import classNames from "classnames";
import AnalyzersList from "./ServerWideCustomAnalyzersList";

todo("Limits", "Damian", "Get limit from license selector");

export default function ServerWideCustomAnalyzers() {
    const { manageServerService } = useServices();
    const asyncGetAnalyzers = useAsync(manageServerService.getServerWideCustomAnalyzers, []);

    const { appUrl } = useAppUrls();

    const isCommunity = useAppSelector(licenseSelectors.licenseType) === "Community";
    const communityLimit = 5; // TODO get from license selector

    const resultsCount = asyncGetAnalyzers.result?.length ?? null;
    const isAddDisabled = asyncGetAnalyzers.status !== "success" || (isCommunity && resultsCount === communityLimit);

    return (
        <div className="content-margin">
            <Col xxl={12}>
                <Row className="gy-sm">
                    <Col>
                        <AboutViewHeading title="Server-Wide Analyzers" icon="server-wide-custom-analyzers" />
                        <a
                            href={appUrl.forEditServerWideCustomAnalyzer()}
                            className={classNames("btn btn-primary mb-3", { disabled: isAddDisabled })}
                        >
                            <Icon icon="plus" />
                            Add a server-wide custom analyzer
                        </a>
                        <HrHeader>
                            Server-wide custom analyzers
                            {isCommunity && (
                                <CounterBadge className="ms-2" count={resultsCount} limit={communityLimit} />
                            )}
                        </HrHeader>
                        <AnalyzersList
                            fetchStatus={asyncGetAnalyzers.status}
                            analyzers={asyncGetAnalyzers.result}
                            reload={asyncGetAnalyzers.execute}
                        />
                    </Col>
                    <Col sm={12} lg={4}>
                        <AboutViewAnchored>
                            <AccordionItemWrapper
                                targetId="1"
                                icon="about"
                                color="info"
                                description="Get additional info on this feature"
                                heading="About this view"
                            >
                                <p>
                                    <strong>Analyzers</strong> are used by indexes to split the index-fields into tokens
                                    (terms).
                                    <br />
                                    The analyzer defines how the field is tokenized.
                                    <br />
                                    When querying an index, these terms are used to define the search criteria and
                                    filter query results.
                                </p>
                                <p>
                                    <strong>In this view</strong>, you can add your own analyzers in addition to the
                                    existing analyzers that come with RavenDB.
                                    <ul>
                                        <li>
                                            The custom analyzers added here can be used by indexes in ALL databases in
                                            your cluster.
                                        </li>
                                        <li>Note: custom analyzers are not supported by Corax indexes.</li>
                                    </ul>
                                </p>
                                <p>
                                    Provide <code>C#</code> code in the editor view, or upload from file.
                                    <ul>
                                        <li>
                                            The analyzer name must be the same as the analyzer&apos;s class name in your
                                            code.
                                        </li>
                                        <li>
                                            Inherit from <code>Lucene.Net.Analysis.Analyzer</code>
                                        </li>
                                        <li>
                                            Code must be compilable and include all necessary <code>using</code>{" "}
                                            statements.
                                        </li>
                                    </ul>
                                </p>
                                <hr />
                                <div className="small-label mb-2">useful links</div>
                                <a href="https://ravendb.net/l/VWCQPI/latest" target="_blank">
                                    <Icon icon="newtab" /> Docs - Custom Analyzers
                                </a>
                            </AccordionItemWrapper>
                            {isCommunity && (
                                <AccordionLicenseLimited
                                    targetId="licensing"
                                    featureName="Custom Analyzers"
                                    featureIcon="server-wide-custom-analyzers"
                                    description="Upgrade to a paid plan and get unlimited availability."
                                />
                            )}
                        </AboutViewAnchored>
                    </Col>
                </Row>
            </Col>
        </div>
    );
}
