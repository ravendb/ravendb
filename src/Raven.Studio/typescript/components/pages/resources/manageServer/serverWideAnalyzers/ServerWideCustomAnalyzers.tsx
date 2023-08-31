import React from "react";
import { Button, Col, Row } from "reactstrap";
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
import { useServices } from "components/hooks/useServices";
import { AsyncStateStatus, useAsync, useAsyncCallback } from "react-async-hook";
import { LoadingView } from "components/common/LoadingView";
import { LoadError } from "components/common/LoadError";
import ButtonWithSpinner from "components/common/ButtonWithSpinner";
import AccordionCommunityLicenseLimited from "components/common/AccordionCommunityLicenseLimited";
import { todo } from "common/developmentHelper";
import { useAppUrls } from "components/hooks/useAppUrls";

todo("Feature", "Damian", "Get limit from license selector");

export default function ServerWideCustomAnalyzers() {
    const { manageServerService } = useServices();
    const asyncGetConfig = useAsync(manageServerService.getServerWideCustomAnalyzers, []);

    const limit = 5; // TODO get from license selector

    return (
        <div className="content-margin">
            <Col xxl={12}>
                <Row className="gy-sm">
                    <Col>
                        <AboutViewHeading title="Server-Wide Analyzers" icon="server-wide-custom-analyzers" />
                        <Button
                            color="primary"
                            className="mb-3"
                            disabled={asyncGetConfig.status !== "success" || asyncGetConfig.result?.length === limit}
                        >
                            <Icon icon="plus" />
                            Add a server-wide custom analyzer
                        </Button>
                        <HrHeader count={asyncGetConfig.result?.length ?? null} limit={limit}>
                            Server-wide custom analyzers
                        </HrHeader>
                        <AnalyzersList
                            fetchStatus={asyncGetConfig.status}
                            analyzers={asyncGetConfig.result}
                            reload={asyncGetConfig.execute}
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
                                TODO
                            </AccordionItemWrapper>
                            {asyncGetConfig.result?.length === limit && (
                                <AccordionCommunityLicenseLimited
                                    targetId="licensing"
                                    featureName="Custom Analyzers"
                                    featureIcon="custom-analyzers"
                                    description="You've reached the limit of 1 database custom analyzer for Community license. Upgrade to a paid plan and get unlimited availability."
                                />
                            )}
                        </AboutViewAnchored>
                    </Col>
                </Row>
            </Col>
        </div>
    );
}

interface AnalyzersListProps {
    fetchStatus: AsyncStateStatus;
    analyzers: Raven.Client.Documents.Indexes.Analysis.AnalyzerDefinition[];
    reload: () => void;
}

function AnalyzersList({ fetchStatus, analyzers, reload }: AnalyzersListProps) {
    const { manageServerService } = useServices();

    const asyncDeleteConfig = useAsyncCallback(manageServerService.deleteServerWideCustomAnalyzer, {
        onSuccess: reload,
    });

    const { appUrl } = useAppUrls();

    if (fetchStatus === "loading") {
        return <LoadingView />;
    }

    if (fetchStatus === "error") {
        return <LoadError error="Unable to load custom analyzers" refresh={reload} />;
    }

    if (analyzers.length === 0) {
        return <EmptySet>No server-wide custom analyzers have been defined</EmptySet>;
    }

    todo("Feature", "Damian", "Render react edit analyzer");

    return (
        <div>
            {analyzers.map((analyzer) => (
                <RichPanel key={analyzer.Name} className="mt-3">
                    <RichPanelHeader>
                        <RichPanelInfo>
                            <RichPanelName>{analyzer.Name}</RichPanelName>
                        </RichPanelInfo>
                        <RichPanelActions>
                            <a
                                href={appUrl.forEditServerWideCustomAnalyzer(analyzer.Name)}
                                className="btn btn-secondary"
                            >
                                <Icon icon="edit" margin="m-0" />
                            </a>
                            <ButtonWithSpinner
                                color="danger"
                                onClick={() => asyncDeleteConfig.execute(analyzer.Name)}
                                icon="trash"
                                isSpinning={asyncDeleteConfig.status === "loading"}
                            />
                        </RichPanelActions>
                    </RichPanelHeader>
                </RichPanel>
            ))}
        </div>
    );
}
