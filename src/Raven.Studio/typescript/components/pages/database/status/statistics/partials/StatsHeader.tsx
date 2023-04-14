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
import { Icon } from "components/common/Icon";

export function StatsHeader() {
    const dispatch = useAppDispatch();

    const detailsVisible = useAppSelector(selectDetailsVisible);
    const spinnerRefresh = useAppSelector(selectRefreshing);

    return (
        <StickyHeader>
            <Row>
                <Col sm="auto">
                    <Button
                        color="secondary"
                        onClick={() => dispatch(toggleDetails())}
                        title="Click to load detailed statistics"
                    >
                        <Icon icon={detailsVisible ? "collapse-vertical" : "expand-vertical"} />
                        <span>{detailsVisible ? "Hide" : "Show"} detailed database &amp; indexing stats</span>
                    </Button>
                </Col>
                <Col />
                <Col sm="auto">
                    <Button
                        color="primary"
                        onClick={() => dispatch(refresh())}
                        disabled={spinnerRefresh}
                        title="Click to refresh stats"
                    >
                        {spinnerRefresh ? <Spinner size="sm" /> : <Icon icon="refresh" margin="m-0" />}
                        <span>Refresh</span>
                    </Button>
                </Col>
            </Row>
        </StickyHeader>
    );
}
