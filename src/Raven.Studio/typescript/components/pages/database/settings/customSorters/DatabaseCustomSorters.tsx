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
import ServerWideCustomSortersList from "components/pages/resources/manageServer/serverWideSorters/ServerWideCustomSortersList";
import { NonShardedViewProps } from "components/models/common";
import FeatureNotAvailable from "components/common/FeatureNotAvailable";
import DeleteCustomSorterConfirm from "components/common/customSorters/DeleteCustomSorterConfirm";
import { accessManagerSelectors } from "components/common/shell/accessManagerSlice";

todo("Feature", "Damian", "Add 'Test custom sorter' button");
todo("Limits", "Damian", "Get limit from license selector");

export default function DatabaseCustomSorters({ db }: NonShardedViewProps) {
    const { databasesService, manageServerService } = useServices();

    const asyncGetServerWideSorters = useAsync(manageServerService.getServerWideCustomSorters, []);
    const asyncGetDatabaseSorters = useAsync(() => databasesService.getCustomSorters(db), [db]);

    const { appUrl } = useAppUrls();

    const isDatabaseAdmin =
        useAppSelector(accessManagerSelectors.effectiveDatabaseAccessLevel(db.name)) === "DatabaseAdmin";

    const isCommunity = useAppSelector(licenseSelectors.licenseType) === "Community";
    const communityServerWideLimit = 5; // TODO get from license selector
    const communityDatabaseLimit = 1; // TODO get from license selector

    const databaseResultsCount = asyncGetDatabaseSorters.result?.length ?? null;
    const serverWideResultsCount = asyncGetServerWideSorters.result?.length ?? null;

    const isAddDisabled =
        asyncGetDatabaseSorters.status !== "success" ||
        (isCommunity && databaseResultsCount === communityDatabaseLimit);

    if (db.isSharded()) {
        return (
            <FeatureNotAvailable>
                <span>
                    Custom sorters are not available for <Icon icon="sharding" color="shard" margin="m-0" /> sharded
                    databases
                </span>
            </FeatureNotAvailable>
        );
    }

    return (
        <div className="content-margin">
            <Col xxl={12}>
                <Row className="gy-sm">
                    <Col>
                        <AboutViewHeading title="Custom sorters" icon="custom-sorters" />
                        {isDatabaseAdmin && (
                            <a
                                href={appUrl.forEditCustomSorter(db)}
                                className={classNames("btn btn-primary mb-3", { disabled: isAddDisabled })}
                            >
                                <Icon icon="plus" />
                                Add a custom sorter
                            </a>
                        )}
                        <HrHeader
                            right={
                                <a href="https://ravendb.net/l/LGUJH8/6.0" target="_blank">
                                    <Icon icon="link" />
                                    Sorters tutorial
                                </a>
                            }
                        >
                            Server-wide custom sorters
                            {isCommunity && (
                                <CounterBadge
                                    className="ms-2"
                                    count={databaseResultsCount}
                                    limit={communityDatabaseLimit}
                                />
                            )}
                        </HrHeader>
                        <DatabaseSortersList
                            fetchStatus={asyncGetDatabaseSorters.status}
                            sorters={asyncGetDatabaseSorters.result}
                            reload={asyncGetDatabaseSorters.execute}
                            forEditLink={(name) => appUrl.forEditCustomSorter(db, name)}
                            deleteCustomSorter={(name) => databasesService.deleteCustomSorter(db, name)}
                            serverWideSorterNames={asyncGetServerWideSorters.result?.map((x) => x.Name) ?? []}
                            isDatabaseAdmin={isDatabaseAdmin}
                        />

                        <HrHeader
                            right={
                                <a href={appUrl.forServerWideCustomSorters()} target="_blank">
                                    <Icon icon="link" />
                                    Server-wide custom sorters
                                </a>
                            }
                        >
                            Server-wide custom sorters
                            {isCommunity && (
                                <CounterBadge
                                    className="ms-2"
                                    count={serverWideResultsCount}
                                    limit={communityServerWideLimit}
                                />
                            )}
                        </HrHeader>
                        <ServerWideCustomSortersList
                            fetchStatus={asyncGetServerWideSorters.status}
                            sorters={asyncGetServerWideSorters.result}
                            reload={asyncGetServerWideSorters.execute}
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
                                This is the <strong>Custom Sorters</strong> view.
                            </AccordionItemWrapper>
                            {isCommunity && (
                                <AccordionLicenseLimited
                                    targetId="licensing"
                                    featureName="Custom Sorters"
                                    featureIcon="custom-sorters"
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

interface DatabaseSortersListProps {
    fetchStatus: AsyncStateStatus;
    sorters: Raven.Client.Documents.Queries.Sorting.SorterDefinition[];
    reload: () => void;
    forEditLink: (name: string) => string;
    deleteCustomSorter: (name: string) => Promise<void>;
    serverWideSorterNames: string[];
    isDatabaseAdmin: boolean;
}

function DatabaseSortersList({
    forEditLink,
    fetchStatus,
    sorters,
    reload,
    deleteCustomSorter,
    serverWideSorterNames,
    isDatabaseAdmin,
}: DatabaseSortersListProps) {
    const asyncDeleteSorter = useAsyncCallback(deleteCustomSorter, {
        onSuccess: reload,
    });

    const [nameToConfirmDelete, setNameToConfirmDelete] = useState<string>(null);

    if (fetchStatus === "loading") {
        return <LoadingView />;
    }

    if (fetchStatus === "error") {
        return <LoadError error="Unable to load custom sorters" refresh={reload} />;
    }

    if (sorters.length === 0) {
        return <EmptySet>No custom sorters have been defined</EmptySet>;
    }

    todo("Feature", "Damian", "Render react edit sorter");

    return (
        <div>
            {sorters.map((sorter) => {
                const tooltipId = "override-info" + sorter.Name.replace(/\s/g, "-");

                return (
                    <RichPanel key={sorter.Name} className="mt-3">
                        <RichPanelHeader>
                            <RichPanelInfo>
                                <RichPanelName>{sorter.Name}</RichPanelName>
                            </RichPanelInfo>
                            {serverWideSorterNames.includes(sorter.Name) && (
                                <>
                                    <UncontrolledTooltip target={tooltipId} placement="left">
                                        Overrides server-wide sorter
                                    </UncontrolledTooltip>
                                    <Icon id={tooltipId} icon="info" />
                                </>
                            )}
                            <RichPanelActions>
                                <a href={forEditLink(sorter.Name)} className="btn btn-secondary">
                                    <Icon icon={isDatabaseAdmin ? "edit" : "preview"} margin="m-0" />
                                </a>

                                {isDatabaseAdmin && (
                                    <>
                                        {nameToConfirmDelete != null && (
                                            <DeleteCustomSorterConfirm
                                                name={nameToConfirmDelete}
                                                onConfirm={(name) => asyncDeleteSorter.execute(name)}
                                                toggle={() => setNameToConfirmDelete(null)}
                                            />
                                        )}
                                        <ButtonWithSpinner
                                            color="danger"
                                            onClick={() => setNameToConfirmDelete(sorter.Name)}
                                            icon="trash"
                                            isSpinning={asyncDeleteSorter.status === "loading"}
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
