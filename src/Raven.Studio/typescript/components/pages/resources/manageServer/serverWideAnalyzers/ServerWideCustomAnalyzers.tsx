import React from "react";
import { Col, Row } from "reactstrap";
import { AboutViewAnchored, AboutViewHeading, AccordionItemWrapper } from "components/common/AboutView";
import { Icon } from "components/common/Icon";
import { HrHeader } from "components/common/HrHeader";
import { useServices } from "components/hooks/useServices";
import { useAsync } from "react-async-hook";
import AccordionCommunityLicenseLimited from "components/common/AccordionCommunityLicenseLimited";
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
                        <HrHeader
                            right={
                                <a href="https://ravendb.net/l/VWCQPI/6.0" target="_blank">
                                    <Icon icon="link" />
                                    Analyzers tutorial
                                </a>
                            }
                        >
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
                                description="Get additional info on what this feature can offer you"
                                heading="About this view"
                            >
                                This is the <strong>Server-Wide Custom Analyzers</strong> view.
                            </AccordionItemWrapper>
                            {isCommunity && (
                                <AccordionCommunityLicenseLimited
                                    targetId="licensing"
                                    featureName="Custom Analyzers"
                                    featureIcon="custom-analyzers"
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
