import React, { useState } from "react";
import { Col, Row, UncontrolledPopover, UncontrolledTooltip } from "reactstrap";
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

export default function DatabaseCustomAnalyzers({ db }: NonShardedViewProps) {
    const { databasesService, manageServerService } = useServices();

    const asyncGetServerWideAnalyzers = useAsync(manageServerService.getServerWideCustomAnalyzers, []);
    const asyncGetDatabaseAnalyzers = useAsync(() => databasesService.getCustomAnalyzers(db), [db]);

    const { appUrl } = useAppUrls();

    const isDatabaseAdmin =
        useAppSelector(accessManagerSelectors.effectiveDatabaseAccessLevel(db.name)) === "DatabaseAdmin";

    const isProfessionalOrAbove = useAppSelector(licenseSelectors.isProfessionalOrAbove());

    const licenseClusterLimit = useAppSelector(licenseSelectors.statusValue("MaxNumberOfCustomAnalyzersPerCluster"));
    const licenseDatabaseLimit = useAppSelector(licenseSelectors.statusValue("MaxNumberOfCustomAnalyzersPerDatabase"));

    const databaseResultsCount = asyncGetDatabaseAnalyzers.result?.length ?? null;
    const serverWideResultsCount = asyncGetServerWideAnalyzers.result?.length ?? null;

    const isAddDisabled =
        asyncGetDatabaseAnalyzers.status !== "success" ||
        (!isProfessionalOrAbove && databaseResultsCount === licenseDatabaseLimit);

    const isButtonPopoverVisible = isCommunity && isDatabaseAdmin && isAddDisabled;

    return (
        <div className="content-margin">
            <Col xxl={12}>
                <Row className="gy-sm">
                    <Col>
                        <AboutViewHeading title="Custom analyzers" icon="custom-analyzers" />
                        {isDatabaseAdmin && (
                            <div id="newCustomAnalyzer" className="w-fit-content">
                                <a
                                    href={appUrl.forEditCustomAnalyzer(db)}
                                    className={classNames("btn btn-primary mb-3", { disabled: isAddDisabled })}
                                >
                                    <Icon icon="plus" />
                                    Add a custom analyzer
                                </a>
                            </div>
                        )}
                        {isButtonPopoverVisible && (
                            <UncontrolledPopover
                                trigger="hover"
                                target="newCustomAnalyzer"
                                placement="top"
                                className="bs5"
                            >
                                <div className="p-3 text-center">
                                    Database has reached the maximum number of Custom Analyzers allowed per database.
                                    <br /> Delete unused analyzers or{" "}
                                    <a href="https://ravendb.net/l/FLDLO4/6.0" target="_blank">
                                        upgrade your license
                                    </a>
                                </div>
                            </UncontrolledPopover>
                        )}
                        <HrHeader>
                            Database custom analyzers
                            {!isProfessionalOrAbove && (
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
                            forEditLink={(name) => appUrl.forEditCustomAnalyzer(db, name)}
                            deleteCustomAnalyzer={(name) => databasesService.deleteCustomAnalyzer(db, name)}
                            serverWideAnalyzerNames={asyncGetServerWideAnalyzers.result?.map((x) => x.Name) ?? []}
                            isDatabaseAdmin={isDatabaseAdmin}
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
                        >
                            Server-wide custom analyzers
                            {!isProfessionalOrAbove && (
                                <CounterBadge
                                    className="ms-2"
                                    count={serverWideResultsCount}
                                    limit={licenseClusterLimit}
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
                                            The custom analyzers added here can be used only by indexes in this
                                            database.
                                        </li>
                                        <li>
                                            The server-wide custom analyzers listed can also be used in this database.
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
                                <a href="https://ravendb.net/l/VWCQPI/latest" target="_blank">
                                    <Icon icon="newtab" /> Docs - Custom Analyzers
                                </a>
                            </AccordionItemWrapper>
                            <AccordionLicenseLimited
                                targetId="licensing"
                                featureName="Custom Analyzers"
                                featureIcon="custom-analyzers"
                                description="Upgrade to a paid plan and get unlimited availability."
                                isLimited={!isProfessionalOrAbove}
                            />
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
