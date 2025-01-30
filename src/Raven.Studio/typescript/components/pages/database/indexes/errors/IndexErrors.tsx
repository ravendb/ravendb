import React from "react";
import { Col, Row } from "reactstrap";
import { AboutViewHeading } from "components/common/AboutView";
import IndexErrorsActions from "components/pages/database/indexes/errors/IndexErrorsActions";
import { IndexErrorsPanel } from "components/pages/database/indexes/errors/IndexErrorsPanel";
import { useIndexErrors } from "components/pages/database/indexes/errors/hooks/useIndexErrors";
import useIndexErrorsTable from "components/pages/database/indexes/errors/hooks/useIndexErrorsTable";
import IndexErrorsAboutView from "components/pages/database/indexes/errors/IndexErrorsAboutView";

export default function IndexErrors() {
    const { indexErrorsPanelTable } = useIndexErrorsTable();
    const { errorInfoItems, asyncFetchAllErrorCount, erroredIndexNames, erroredActionNames } = useIndexErrors();

    return (
        <div className="content-margin">
            <Col xxl={12}>
                <Row className="gy-sm">
                    <Col>
                        <div className="flex-shrink-0 hstack gap-2 align-items-start">
                            <AboutViewHeading icon="index-errors" title="Index Errors" />
                        </div>
                        <IndexErrorsActions
                            refresh={asyncFetchAllErrorCount.execute}
                            isLoading={asyncFetchAllErrorCount.loading}
                            erroredActionNames={erroredActionNames}
                            erroredIndexNames={erroredIndexNames}
                            filters={indexErrorsPanelTable.getState().columnFilters}
                            setFilters={indexErrorsPanelTable.setColumnFilters}
                        />
                        {errorInfoItems.map((errorItem, index) => (
                            <IndexErrorsPanel
                                key={index}
                                asyncFetchAllErrorCount={asyncFetchAllErrorCount}
                                errorItem={errorItem}
                                table={indexErrorsPanelTable}
                                nodeTag={errorItem.location.nodeTag}
                                shardNumber={errorItem.location.shardNumber}
                                totalErrorsCount={errorItem.totalErrorCount}
                            />
                        ))}
                    </Col>
                    {/*TODO Until Danielle adds the text, this component will remain disabled so as not to block the possibility of it being merged.*/}
                    {false && (
                        <Col sm={12} lg={4}>
                            <IndexErrorsAboutView />
                        </Col>
                    )}
                </Row>
            </Col>
        </div>
    );
}
