import React from "react";
import { Col, Row } from "reactstrap";
import { AboutViewAnchored, AboutViewHeading, AccordionItemWrapper } from "components/common/AboutView";
import { Icon } from "components/common/Icon";
import { HrHeader } from "components/common/HrHeader";
import { useAsync } from "react-async-hook";
import { useAppUrls } from "components/hooks/useAppUrls";
import { useServices } from "components/hooks/useServices";
import { useAppSelector } from "components/store";
import { licenseSelectors } from "components/common/shell/licenseSlice";
import { CounterBadge } from "components/common/CounterBadge";
import AccordionLicenseLimited from "components/common/AccordionLicenseLimited";
import classNames from "classnames";
import SortersList from "./ServerWideCustomSortersList";
import { todo } from "common/developmentHelper";

todo("Limits", "Damian", "Get limit from license selector");

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
                            href={appUrl.forEditServerWideCustomSorter()}
                            className={classNames("btn btn-primary mb-3", { disabled: isAddDisabled })}
                        >
                            <Icon icon="plus" />
                            Add a server-wide custom sorter
                        </a>
                        <HrHeader>
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
                                description="Get additional info on this feature"
                                heading="About this view"
                            >
                                <p>
                                    A <strong>Custom Sorter</strong> allows you to define how documents will be ordered
                                    in the query results according to your specific requirements.
                                </p>
                                <p>
                                    <strong>In this view</strong>, you can add your own sorters:
                                    <ul className="margin-top-xxs">
                                        <li>
                                            The custom sorters added here can be used with queries in ALL databases in
                                            your cluster.
                                        </li>
                                        <li>Note: custom sorters are not supported when querying Corax indexes.</li>
                                    </ul>
                                </p>
                                <p>
                                    Provide <code>C#</code> code in the editor view, or upload from file:
                                    <ul className="margin-top-xxs">
                                        <li>
                                            The sorter name must be the same as the sorter&apos;s class name in your
                                            code.
                                        </li>
                                        <li>
                                            Inherit from <code>Lucene.Net.Search.FieldComparator</code>
                                        </li>
                                        <li>
                                            Code must be compilable and include all necessary <code>using</code>{" "}
                                            statements.
                                        </li>
                                    </ul>
                                </p>
                                <hr />
                                <div className="small-label mb-2">useful links</div>
                                <a href="https://ravendb.net/l/LGUJH8/latest" target="_blank">
                                    <Icon icon="newtab" /> Docs - Custom Sorters
                                </a>
                            </AccordionItemWrapper>
                            {isCommunity && (
                                <AccordionLicenseLimited
                                    targetId="licensing"
                                    featureName="Custom Sorters"
                                    featureIcon="server-wide-custom-sorters"
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
