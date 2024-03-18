import React, { useEffect, useState } from "react";
import { Alert, Col, Row, UncontrolledPopover, UncontrolledTooltip } from "reactstrap";
import { AboutViewAnchored, AboutViewHeading, AccordionItemWrapper } from "components/common/AboutView";
import { Icon } from "components/common/Icon";
import { HrHeader } from "components/common/HrHeader";
import { EmptySet } from "components/common/EmptySet";
import {
    RichPanel,
    RichPanelActions,
    RichPanelHeader,
    RichPanelInfo,
    RichPanelName,
} from "components/common/RichPanel";
import { todo } from "common/developmentHelper";
import { licenseSelectors } from "components/common/shell/licenseSlice";
import { useAppUrls } from "components/hooks/useAppUrls";
import { useServices } from "components/hooks/useServices";
import { useAppSelector } from "components/store";
import { AsyncStateStatus, useAsync, useAsyncCallback } from "react-async-hook";
import classNames from "classnames";
import ButtonWithSpinner from "components/common/ButtonWithSpinner";
import { CounterBadge } from "components/common/CounterBadge";
import { LoadError } from "components/common/LoadError";
import { LoadingView } from "components/common/LoadingView";
import ServerWideCustomAnalyzersList from "components/pages/resources/manageServer/serverWideAnalyzers/ServerWideCustomAnalyzersList";
import DeleteCustomAnalyzerConfirm from "components/common/customAnalyzers/DeleteCustomAnalyzerConfirm";
import { accessManagerSelectors } from "components/common/shell/accessManagerSlice";
import { getLicenseLimitReachStatus, useLimitedFeatureAvailability } from "components/utils/licenseLimitsUtils";
import { useRavenLink } from "components/hooks/useRavenLink";
import FeatureAvailabilitySummaryWrapper, {
    FeatureAvailabilityData,
} from "components/common/FeatureAvailabilitySummary";
import { throttledUpdateLicenseLimitsUsage } from "components/common/shell/setup";
import LicenseRestrictedBadge from "components/common/LicenseRestrictedBadge";
import { databaseSelectors } from "components/common/shell/databaseSliceSelectors";

export default function DatabaseCustomAnalyzers() {
    const databaseName = useAppSelector(databaseSelectors.activeDatabaseName);
    const isDatabaseAdminOrAbove = useAppSelector(accessManagerSelectors.isDatabaseAdminOrAbove());

    const { databasesService, manageServerService } = useServices();

    const asyncGetServerWideAnalyzers = useAsync(manageServerService.getServerWideCustomAnalyzers, []);
    const asyncGetDatabaseAnalyzers = useAsync(() => databasesService.getCustomAnalyzers(databaseName), [databaseName]);

    const { appUrl } = useAppUrls();
    const upgradeLicenseLink = useRavenLink({ hash: "FLDLO4", isDocs: false });
    const customAnalyzersDocsLink = useRavenLink({ hash: "VWCQPI" });

    const licenseClusterLimit = useAppSelector(licenseSelectors.statusValue("MaxNumberOfCustomAnalyzersPerCluster"));
    const licenseDatabaseLimit = useAppSelector(licenseSelectors.statusValue("MaxNumberOfCustomAnalyzersPerDatabase"));
    const numberOfCustomAnalyzersInCluster = useAppSelector(licenseSelectors.limitsUsage).NumberOfAnalyzersInCluster;
    const hasServerWideCustomAnalyzers = useAppSelector(licenseSelectors.statusValue("HasServerWideAnalyzers"));

    const featureAvailability = useLimitedFeatureAvailability({
        defaultFeatureAvailability,
        overwrites: [
            {
                featureName: defaultFeatureAvailability[0].featureName,
                value: licenseDatabaseLimit,
            },
            {
                featureName: defaultFeatureAvailability[1].featureName,
                value: licenseClusterLimit,
            },
            {
                featureName: defaultFeatureAvailability[2].featureName,
                value: hasServerWideCustomAnalyzers,
            },
        ],
    });

    const databaseResultsCount = asyncGetDatabaseAnalyzers.result?.length ?? null;
    const serverWideResultsCount = asyncGetServerWideAnalyzers.result?.length ?? null;

    useEffect(() => {
        throttledUpdateLicenseLimitsUsage();
    }, [databaseResultsCount]);

    const databaseLimitReachStatus = getLicenseLimitReachStatus(databaseResultsCount, licenseDatabaseLimit);
    const clusterLimitReachStatus = getLicenseLimitReachStatus(numberOfCustomAnalyzersInCluster, licenseClusterLimit);

    const isLimitReached = databaseLimitReachStatus === "limitReached" || clusterLimitReachStatus === "limitReached";

    return (
        <>
            {databaseLimitReachStatus !== "notReached" && (
                <Alert
                    color={databaseLimitReachStatus === "limitReached" ? "danger" : "warning"}
                    className="text-center mb-3"
                >
                    <Icon icon="database" />
                    Database {databaseLimitReachStatus === "limitReached" ? "has reached" : "is reaching"} the{" "}
                    <strong>maximum number of Custom Analyzers</strong> allowed per database by your license{" "}
                    <strong>
                        ({databaseResultsCount}/{licenseDatabaseLimit})
                    </strong>
                    <br /> Delete unused analyzers or{" "}
                    <strong>
                        <a href={upgradeLicenseLink} target="_blank">
                            upgrade your license
                        </a>
                    </strong>
                </Alert>
            )}
            {clusterLimitReachStatus !== "notReached" && (
                <Alert
                    color={clusterLimitReachStatus === "limitReached" ? "danger" : "warning"}
                    className="text-center mb-3"
                >
                    <Icon icon="cluster" />
                    Cluster {clusterLimitReachStatus === "limitReached" ? "has reached" : "is reaching"} the{" "}
                    <strong>maximum number of Custom Analyzers</strong> allowed per cluster by your license{" "}
                    <strong>
                        ({numberOfCustomAnalyzersInCluster}/{licenseClusterLimit})
                    </strong>
                    <br /> Delete unused analyzers or{" "}
                    <strong>
                        <a href={upgradeLicenseLink} target="_blank">
                            upgrade your license
                        </a>
                    </strong>
                </Alert>
            )}
            <div className="content-margin">
                <Col xxl={12}>
                    <Row className="gy-sm">
                        <Col>
                            <AboutViewHeading title="Custom analyzers" icon="custom-analyzers" />
                            {isDatabaseAdminOrAbove && (
                                <>
                                    <div id="newCustomAnalyzer" className="w-fit-content">
                                        <a
                                            href={appUrl.forEditCustomAnalyzer(databaseName)}
                                            className={classNames("btn btn-primary mb-3", { disabled: isLimitReached })}
                                        >
                                            <Icon icon="plus" />
                                            Add a custom analyzer
                                        </a>
                                    </div>
                                    {isLimitReached && (
                                        <UncontrolledPopover
                                            trigger="hover"
                                            target="newCustomAnalyzer"
                                            placement="top"
                                            className="bs5"
                                        >
                                            <div className="p-3 text-center">
                                                <Icon
                                                    icon={
                                                        databaseLimitReachStatus === "limitReached"
                                                            ? "database"
                                                            : "cluster"
                                                    }
                                                />
                                                {databaseLimitReachStatus === "limitReached" ? "Database" : "Cluster"}{" "}
                                                has reached the maximum number of Custom Analyzers allowed per{" "}
                                                {databaseLimitReachStatus === "limitReached" ? "database" : "cluster"}.
                                                <br /> Delete unused analyzers or{" "}
                                                <a href={upgradeLicenseLink} target="_blank">
                                                    upgrade your license
                                                </a>
                                            </div>
                                        </UncontrolledPopover>
                                    )}
                                </>
                            )}

                            <HrHeader count={databaseLimitReachStatus === "notReached" ? databaseResultsCount : null}>
                                Database custom analyzers
                                {databaseLimitReachStatus !== "notReached" && (
                                    <CounterBadge
                                        className="ms-2"
                                        count={databaseResultsCount}
                                        limit={licenseDatabaseLimit}
                                    />
                                )}
                            </HrHeader>
                            <DatabaseAnalyzersList
                                fetchStatus={asyncGetDatabaseAnalyzers.status}
                                analyzers={asyncGetDatabaseAnalyzers.result}
                                reload={asyncGetDatabaseAnalyzers.execute}
                                forEditLink={(name) => appUrl.forEditCustomAnalyzer(databaseName, name)}
                                deleteCustomAnalyzer={(name) =>
                                    databasesService.deleteCustomAnalyzer(databaseName, name)
                                }
                                serverWideAnalyzerNames={asyncGetServerWideAnalyzers.result?.map((x) => x.Name) ?? []}
                                isDatabaseAdmin={isDatabaseAdminOrAbove}
                            />

                            <HrHeader
                                right={
                                    <a
                                        href={appUrl.forServerWideCustomAnalyzers()}
                                        target="_blank"
                                        title="Navigate to the server-wide view to edit"
                                    >
                                        <Icon icon="link" />
                                        Server-wide custom analyzers
                                    </a>
                                }
                                count={serverWideResultsCount}
                            >
                                Server-wide custom analyzers
                                {!hasServerWideCustomAnalyzers && (
                                    <LicenseRestrictedBadge licenseRequired="Professional +" />
                                )}
                            </HrHeader>
                            {hasServerWideCustomAnalyzers && (
                                <ServerWideCustomAnalyzersList
                                    fetchStatus={asyncGetServerWideAnalyzers.status}
                                    analyzers={asyncGetServerWideAnalyzers.result}
                                    reload={asyncGetServerWideAnalyzers.execute}
                                    isReadOnly
                                />
                            )}
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
                                        <strong>Analyzers</strong> are used by indexes to split the index-fields into
                                        tokens (terms).
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
                                                The custom analyzers added here can be used only by indexes in this
                                                database.
                                            </li>
                                            <li>
                                                The server-wide custom analyzers listed can also be used in this
                                                database.
                                            </li>
                                            <li>Note: custom analyzers are not supported by Corax indexes.</li>
                                        </ul>
                                    </div>
                                    <div>
                                        Provide <code>C#</code> code in the editor view, or upload from file.
                                        <ul>
                                            <li>
                                                The analyzer name must be the same as the analyzer&apos;s class name in
                                                your code.
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
                                    isUnlimited={
                                        databaseLimitReachStatus === "notReached" &&
                                        clusterLimitReachStatus === "notReached" &&
                                        hasServerWideCustomAnalyzers
                                    }
                                    data={featureAvailability}
                                />
                            </AboutViewAnchored>
                        </Col>
                    </Row>
                </Col>
            </div>
        </>
    );
}

interface DatabaseAnalyzersListProps {
    fetchStatus: AsyncStateStatus;
    analyzers: Raven.Client.Documents.Indexes.Analysis.AnalyzerDefinition[];
    reload: () => void;
    forEditLink: (name: string) => string;
    deleteCustomAnalyzer: (name: string) => Promise<void>;
    serverWideAnalyzerNames: string[];
    isDatabaseAdmin: boolean;
}

function DatabaseAnalyzersList({
    forEditLink,
    fetchStatus,
    analyzers,
    reload,
    deleteCustomAnalyzer,
    serverWideAnalyzerNames,
    isDatabaseAdmin,
}: DatabaseAnalyzersListProps) {
    const asyncDeleteAnalyzer = useAsyncCallback(deleteCustomAnalyzer, {
        onSuccess: reload,
    });

    const [nameToConfirmDelete, setNameToConfirmDelete] = useState<string>(null);

    if (fetchStatus === "loading") {
        return <LoadingView />;
    }

    if (fetchStatus === "error") {
        return <LoadError error="Unable to load custom analyzers" refresh={reload} />;
    }

    if (analyzers.length === 0) {
        return <EmptySet>No custom analyzers have been defined</EmptySet>;
    }

    todo("Feature", "Damian", "Render react edit analyzer");

    return (
        <div>
            {analyzers.map((analyzer) => {
                const tooltipId = "override-info" + analyzer.Name.replace(/\s/g, "-");

                return (
                    <RichPanel key={analyzer.Name} className="mt-3">
                        <RichPanelHeader>
                            <RichPanelInfo>
                                <RichPanelName>{analyzer.Name}</RichPanelName>
                            </RichPanelInfo>
                            {serverWideAnalyzerNames.includes(analyzer.Name) && (
                                <>
                                    <UncontrolledTooltip target={tooltipId} placement="left">
                                        Overrides server-wide analyzer
                                    </UncontrolledTooltip>
                                    <Icon id={tooltipId} icon="info" color="info" />
                                </>
                            )}
                            <RichPanelActions>
                                <a href={forEditLink(analyzer.Name)} className="btn btn-secondary">
                                    <Icon icon={isDatabaseAdmin ? "edit" : "preview"} margin="m-0" />
                                </a>

                                {isDatabaseAdmin && (
                                    <>
                                        {nameToConfirmDelete != null && (
                                            <DeleteCustomAnalyzerConfirm
                                                name={nameToConfirmDelete}
                                                onConfirm={(name) => asyncDeleteAnalyzer.execute(name)}
                                                toggle={() => setNameToConfirmDelete(null)}
                                            />
                                        )}
                                        <ButtonWithSpinner
                                            color="danger"
                                            onClick={() => setNameToConfirmDelete(analyzer.Name)}
                                            icon="trash"
                                            isSpinning={asyncDeleteAnalyzer.status === "loading"}
                                        />
                                    </>
                                )}
                            </RichPanelActions>
                        </RichPanelHeader>
                    </RichPanel>
                );
            })}
        </div>
    );
}

const defaultFeatureAvailability: FeatureAvailabilityData[] = [
    {
        featureName: "Limit per database",
        community: { value: 1 },
        professional: { value: Infinity },
        enterprise: { value: Infinity },
    },
    {
        featureName: "Limit per cluster",
        community: { value: 5 },
        professional: { value: Infinity },
        enterprise: { value: Infinity },
    },
    {
        featureName: "Server-wide custom analyzers",
        community: { value: false },
        professional: { value: true },
        enterprise: { value: true },
    },
];
