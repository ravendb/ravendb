import { Button, Col, Row } from "reactstrap";
import {
    refresh,
    statisticsViewSelectors,
    toggleDetails,
} from "components/pages/database/status/statistics/store/statisticsViewSlice";
import { StickyHeader } from "components/common/StickyHeader";
import React from "react";
import { useAppDispatch, useAppSelector } from "components/store";
import { Icon } from "components/common/Icon";
import ButtonWithSpinner from "components/common/ButtonWithSpinner";

export function StatsHeader() {
    const dispatch = useAppDispatch();

    const detailsVisible = useAppSelector(statisticsViewSelectors.detailsVisible);
    const isRefreshing = useAppSelector(statisticsViewSelectors.refreshing);

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
                    <ButtonWithSpinner
                        color="primary"
                        onClick={() => dispatch(refresh())}
                        title="Click to refresh stats"
                        isSpinning={isRefreshing}
                    >
                        Refresh
                    </ButtonWithSpinner>
                </Col>
            </Row>
        </StickyHeader>
    );
}
