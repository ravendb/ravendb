import React from "react";
import { Col, Row, UncontrolledPopover } from "reactstrap";
import { AboutViewAnchored, AboutViewHeading, AccordionItemWrapper } from "components/common/AboutView";
import { Icon } from "components/common/Icon";
import { HrHeader } from "components/common/HrHeader";
import { useServices } from "components/hooks/useServices";
import { useAsync } from "react-async-hook";
import { useAppUrls } from "components/hooks/useAppUrls";
import { useAppSelector } from "components/store";
import { licenseSelectors } from "components/common/shell/licenseSlice";
import { CounterBadge } from "components/common/CounterBadge";
import classNames from "classnames";
import AnalyzersList from "./ServerWideCustomAnalyzersList";
import { getLicenseLimitReachStatus } from "components/utils/licenseLimitsUtils";
import { useRavenLink } from "components/hooks/useRavenLink";
import FeatureAvailabilitySummaryWrapper, {
    FeatureAvailabilityData,
} from "components/common/FeatureAvailabilitySummary";

export default function ServerWideCustomAnalyzers() {
    const { manageServerService } = useServices();
    const asyncGetAnalyzers = useAsync(manageServerService.getServerWideCustomAnalyzers, []);

    const { appUrl } = useAppUrls();
    const upgradeLicenseLink = useRavenLink({ hash: "FLDLO4", isDocs: false });
    const customAnalyzersDocsLink = useRavenLink({ hash: "VWCQPI" });

    const isProfessionalOrAbove = useAppSelector(licenseSelectors.isProfessionalOrAbove());
    const licenseLimit = useAppSelector(licenseSelectors.statusValue("MaxNumberOfCustomAnalyzersPerCluster"));

    const resultsCount = asyncGetAnalyzers.result?.length ?? null;

    const isLimitExceeded =
        !isProfessionalOrAbove && getLicenseLimitReachStatus(resultsCount, licenseLimit) === "limitReached";

    return (
        <div className="content-margin">
            <Col xxl={12}>
                <Row className="gy-sm">
                    <Col>
                        <AboutViewHeading title="Server-Wide Analyzers" icon="server-wide-custom-analyzers" />
                        <div id="newServerWideCustomAnalyzer" className="w-fit-content">
                            <a
                                href={appUrl.forEditServerWideCustomAnalyzer()}
                                className={classNames("btn btn-primary mb-3", { disabled: isLimitExceeded })}
                            >
                                <Icon icon="plus" />
                                Add a server-wide custom analyzer
                            </a>
                        </div>
                        {isLimitExceeded && (
                            <UncontrolledPopover
                                trigger="hover"
                                target="newServerWideCustomAnalyzer"
                                placement="top"
                                className="bs5"
                            >
                                <div className="p-3 text-center">
                                    You&apos;ve reached the maximum number of Custom Analyzers allowed per cluster.
                                    <br /> Delete unused analyzers or{" "}
                                    <a href={upgradeLicenseLink} target="_blank">
                                        upgrade your license
                                    </a>
                                </div>
                            </UncontrolledPopover>
                        )}
                        <HrHeader>
                            Server-wide custom analyzers
                            {!isProfessionalOrAbove && (
                                <CounterBadge className="ms-2" count={resultsCount} limit={licenseLimit} />
                            )}
                        </HrHeader>
                        <AnalyzersList
                            fetchStatus={asyncGetAnalyzers.status}
                            analyzers={asyncGetAnalyzers.result}
                            reload={asyncGetAnalyzers.execute}
                        />
                    </Col>
                    <Col sm={12} lg={4}>
                        <AboutViewAnchored defaultOpen={isProfessionalOrAbove ? null : "licensing"}>
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
                                <div>
                                    <strong>In this view</strong>, you can add your own analyzers in addition to the
                                    existing analyzers that come with RavenDB.
                                    <ul>
                                        <li>
                                            The custom analyzers added here can be used by indexes in ALL databases in
                                            your cluster.
                                        </li>
                                        <li>Note: custom analyzers are not supported by Corax indexes.</li>
                                    </ul>
                                </div>
                                <div>
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
                                </div>
                                <hr />
                                <div className="small-label mb-2">useful links</div>
                                <a href={customAnalyzersDocsLink} target="_blank">
                                    <Icon icon="newtab" /> Docs - Custom Analyzers
                                </a>
                            </AccordionItemWrapper>
                            <FeatureAvailabilitySummaryWrapper
                                isUnlimited={isProfessionalOrAbove}
                                data={featureAvailability}
                            />
                        </AboutViewAnchored>
                    </Col>
                </Row>
            </Col>
        </div>
    );
}

export const featureAvailability: FeatureAvailabilityData[] = [
    {
        featureName: "Analyzers limit",
        community: { value: 5 },
        professional: { value: Infinity },
        enterprise: { value: Infinity },
    },
];
