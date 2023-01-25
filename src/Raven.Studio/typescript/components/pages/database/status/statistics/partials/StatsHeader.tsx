import { Button, Col, Row, Spinner } from "reactstrap";
import {
    refresh,
    selectDetailsVisible,
    selectRefreshing,
    toggleDetails,
} from "components/pages/database/status/statistics/logic/statisticsSlice";
import classNames from "classnames";
import { StickyHeader } from "components/common/StickyHeader";
import React from "react";
import { useAppDispatch, useAppSelector } from "components/store";

export function StatsHeader() {
    const dispatch = useAppDispatch();

    const detailsVisible = useAppSelector(selectDetailsVisible);
    const spinnerRefresh = useAppSelector(selectRefreshing);

    return (
        <StickyHeader>
            <Row>
                <Col />
                <Col sm="auto">
                    <Button
                        color="primary"
                        onClick={() => dispatch(toggleDetails)}
                        title="Click to load detailed statistics"
                    >
                        <i className={classNames(detailsVisible ? "icon-collapse-vertical" : "icon-expand-vertical")} />
                        <span>{detailsVisible ? "Hide" : "Show"} details</span>
                    </Button>
                    <Button
                        color="primary"
                        onClick={() => dispatch(refresh())}
                        disabled={spinnerRefresh}
                        className="margin-left-xs"
                        title="Click to refresh stats"
                    >
                        {spinnerRefresh ? <Spinner size="sm" /> : <i className="icon-refresh"></i>}
                        <span>Refresh</span>
                    </Button>
                </Col>
            </Row>
        </StickyHeader>
    );
}
