import React, { useState } from "react";
import { Col, Row, UncontrolledTooltip } from "reactstrap";
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
import AccordionLicenseLimited from "components/common/AccordionLicenseLimited";
import classNames from "classnames";
import ButtonWithSpinner from "components/common/ButtonWithSpinner";
import { CounterBadge } from "components/common/CounterBadge";
import { LoadError } from "components/common/LoadError";
import { LoadingView } from "components/common/LoadingView";
import ServerWideCustomAnalyzersList from "components/pages/resources/manageServer/serverWideAnalyzers/ServerWideCustomAnalyzersList";
import { NonShardedViewProps } from "components/models/common";
import DeleteCustomAnalyzerConfirm from "components/common/customAnalyzers/DeleteCustomAnalyzerConfirm";
import { accessManagerSelectors } from "components/common/shell/accessManagerSlice";

todo("Limits", "Damian", "Get limit from license selector");

export default function DatabaseCustomAnalyzers({ db }: NonShardedViewProps) {
    const { databasesService, manageServerService } = useServices();

    const asyncGetServerWideAnalyzers = useAsync(manageServerService.getServerWideCustomAnalyzers, []);
    const asyncGetDatabaseAnalyzers = useAsync(() => databasesService.getCustomAnalyzers(db), [db]);

    const { appUrl } = useAppUrls();

    const isDatabaseAdmin =
        useAppSelector(accessManagerSelectors.effectiveDatabaseAccessLevel(db.name)) === "DatabaseAdmin";

    const isCommunity = useAppSelector(licenseSelectors.licenseType) === "Community";
    const communityServerWideLimit = 5; // TODO get from license selector
    const communityDatabaseLimit = 1; // TODO get from license selector

    const databaseResultsCount = asyncGetDatabaseAnalyzers.result?.length ?? null;
    const serverWideResultsCount = asyncGetServerWideAnalyzers.result?.length ?? null;

    const isAddDisabled =
        asyncGetDatabaseAnalyzers.status !== "success" ||
        (isCommunity && databaseResultsCount === communityDatabaseLimit);

    return (
        <div className="content-margin">
            <Col xxl={12}>
                <Row className="gy-sm">
                    <Col>
                        <AboutViewHeading title="Custom analyzers" icon="custom-analyzers" />
                        {isDatabaseAdmin && (
                            <a
                                href={appUrl.forEditCustomAnalyzer(db)}
                                className={classNames("btn btn-primary mb-3", { disabled: isAddDisabled })}
                            >
                                <Icon icon="plus" />
                                Add a custom analyzer
                            </a>
                        )}
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
                                <CounterBadge
                                    className="ms-2"
                                    count={databaseResultsCount}
                                    limit={communityDatabaseLimit}
                                />
                            )}
                        </HrHeader>
                        <DatabaseAnalyzersList
                            fetchStatus={asyncGetDatabaseAnalyzers.status}
                            analyzers={asyncGetDatabaseAnalyzers.result}
                            reload={asyncGetDatabaseAnalyzers.execute}
                            forEditLink={(name) => appUrl.forEditCustomAnalyzer(db, name)}
                            deleteCustomAnalyzer={(name) => databasesService.deleteCustomAnalyzer(db, name)}
                            serverWideAnalyzerNames={asyncGetServerWideAnalyzers.result?.map((x) => x.Name) ?? []}
                            isDatabaseAdmin={isDatabaseAdmin}
                        />

                        <HrHeader
                            right={
                                <a href={appUrl.forServerWideCustomAnalyzers()} target="_blank">
                                    <Icon icon="link" />
                                    Server-wide custom analyzers
                                </a>
                            }
                        >
                            Server-wide custom analyzers
                            {isCommunity && (
                                <CounterBadge
                                    className="ms-2"
                                    count={serverWideResultsCount}
                                    limit={communityServerWideLimit}
                                />
                            )}
                        </HrHeader>
                        <ServerWideCustomAnalyzersList
                            fetchStatus={asyncGetServerWideAnalyzers.status}
                            analyzers={asyncGetServerWideAnalyzers.result}
                            reload={asyncGetServerWideAnalyzers.execute}
                            isReadOnly
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
                                This is the <strong>Custom Analyzers</strong> view.
                            </AccordionItemWrapper>
                            {isCommunity && (
                                <AccordionLicenseLimited
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
                                    <Icon id={tooltipId} icon="info" />
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
