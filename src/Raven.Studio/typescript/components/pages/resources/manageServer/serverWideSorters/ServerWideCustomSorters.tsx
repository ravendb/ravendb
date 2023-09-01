import React from "react";
import { Col, Row } from "reactstrap";
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
import { AsyncStateStatus, useAsync, useAsyncCallback } from "react-async-hook";
import ButtonWithSpinner from "components/common/ButtonWithSpinner";
import { LoadError } from "components/common/LoadError";
import { LoadingView } from "components/common/LoadingView";
import { useAppUrls } from "components/hooks/useAppUrls";
import { useServices } from "components/hooks/useServices";
import { useAppSelector } from "components/store";
import { licenseSelectors } from "components/common/shell/licenseSlice";
import { CounterBadge } from "components/common/CounterBadge";
import AccordionCommunityLicenseLimited from "components/common/AccordionCommunityLicenseLimited";
import classNames from "classnames";

export default function ServerWideCustomSorters() {
    const { manageServerService } = useServices();
    const asyncGetSorters = useAsync(manageServerService.getServerWideCustomSorters, []);

    const { appUrl } = useAppUrls();

    const isCommunity = useAppSelector(licenseSelectors.licenseType) === "Community";
    const communityLimit = 5; // TODO get from license selector

    const resultsCount = asyncGetSorters.result?.length ?? null;
    const isAddDisabled = asyncGetSorters.status !== "success" || (isCommunity && resultsCount === communityLimit);

    return (
        <div className="content-margin">
            <Col xxl={12}>
                <Row className="gy-sm">
                    <Col>
                        <AboutViewHeading title="Server-Wide Sorters" icon="server-wide-custom-sorters" />
                        <a
                            href={appUrl.forEditServerWideCustomAnalyzer()}
                            className={classNames("btn btn-primary mb-3", { disabled: isAddDisabled })}
                        >
                            <Icon icon="plus" />
                            Add a server-wide custom sorter
                        </a>
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
                                <CounterBadge className="ms-2" count={resultsCount} limit={communityLimit} />
                            )}
                        </HrHeader>
                        <SortersList
                            fetchStatus={asyncGetSorters.status}
                            sorters={asyncGetSorters.result}
                            reload={asyncGetSorters.execute}
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
                                Umm
                            </AccordionItemWrapper>
                            {isCommunity && (
                                <AccordionCommunityLicenseLimited
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

interface SortersListProps {
    fetchStatus: AsyncStateStatus;
    sorters: Raven.Client.Documents.Queries.Sorting.SorterDefinition[];
    reload: () => void;
}

function SortersList({ fetchStatus, sorters, reload }: SortersListProps) {
    const { manageServerService } = useServices();

    const asyncDeleteSorter = useAsyncCallback(manageServerService.deleteServerWideCustomSorter, {
        onSuccess: reload,
    });

    const { appUrl } = useAppUrls();

    if (fetchStatus === "loading") {
        return <LoadingView />;
    }

    if (fetchStatus === "error") {
        return <LoadError error="Unable to load custom sorters" refresh={reload} />;
    }

    if (sorters.length === 0) {
        return <EmptySet>No server-wide custom sorters have been defined</EmptySet>;
    }

    todo("Feature", "Damian", "Render react edit sorter");

    return (
        <div>
            {sorters.map((sorter) => (
                <RichPanel key={sorter.Name} className="mt-3">
                    <RichPanelHeader>
                        <RichPanelInfo>
                            <RichPanelName>{sorter.Name}</RichPanelName>
                        </RichPanelInfo>
                        <RichPanelActions>
                            <a href={appUrl.forEditServerWideCustomAnalyzer(sorter.Name)} className="btn btn-secondary">
                                <Icon icon="edit" margin="m-0" />
                            </a>
                            <ButtonWithSpinner
                                color="danger"
                                onClick={() => asyncDeleteSorter.execute(sorter.Name)}
                                icon="trash"
                                isSpinning={asyncDeleteSorter.status === "loading"}
                            />
                        </RichPanelActions>
                    </RichPanelHeader>
                </RichPanel>
            ))}
        </div>
    );
}
