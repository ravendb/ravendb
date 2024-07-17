import React from "react";
import { Alert, Button, Card, CardBody, Col, Row } from "reactstrap";
import { AboutViewHeading } from "components/common/AboutView";
import ButtonWithSpinner from "components/common/ButtonWithSpinner";
import { Icon } from "components/common/Icon";
import { CounterBadge } from "components/common/CounterBadge";
import UsedId from "components/pages/database/settings/unusedDatabaseIds/bits/UsedId";
import UnusedDatabaseIdsAboutView from "components/pages/database/settings/unusedDatabaseIds/bits/UnusedDatabaseIdsAboutView";
import { LazyLoad } from "components/common/LazyLoad";
import UnusedIdsForm from "components/pages/database/settings/unusedDatabaseIds/bits/UnusedIdsForm";
import { useUnusedDatabaseIds } from "components/pages/database/settings/unusedDatabaseIds/useUnusedDatabaseIds";
import PotentialUnusedIdList from "components/pages/database/settings/unusedDatabaseIds/bits/PotentialUnusedIdList";
import "./UnusedDatabaseIds.scss";

export default function UnusedDatabaseIds() {
    const { isDirty, usedIds, unusedIds, unusedIdsActions, potentialUnusedId, isLoading, asyncSaveUnusedDatabaseIDs } =
        useUnusedDatabaseIds();

    return (
        <div className="content-margin unused-database-ids">
            <Row className="gy-sm">
                <Col>
                    <AboutViewHeading title="Unused Database IDs" icon="database-id" />
                    <ButtonWithSpinner
                        type="button"
                        color="primary"
                        className="mb-3"
                        icon="save"
                        onClick={asyncSaveUnusedDatabaseIDs.execute}
                        isSpinning={asyncSaveUnusedDatabaseIDs.loading}
                        disabled={asyncSaveUnusedDatabaseIDs.loading || !isDirty}
                    >
                        Save
                    </ButtonWithSpinner>
                    <Card className="mb-3">
                        <LazyLoad active={isLoading}>
                            <CardBody>
                                <div className="vstack gap-2">
                                    <div className="d-flex gap-1 align-items-center">
                                        <h4 className="mb-0">Used IDs</h4>
                                        <CounterBadge count={usedIds.length} />
                                    </div>
                                    <div className="used-ids-grid">
                                        {usedIds.map((usedId) => (
                                            <UsedId key={usedId.databaseId} usedIdData={usedId} />
                                        ))}
                                    </div>
                                </div>
                            </CardBody>
                        </LazyLoad>
                    </Card>
                    <Card className="mb-3">
                        <LazyLoad active={isLoading}>
                            <CardBody>
                                <div className="vstack gap-2">
                                    <div className="d-flex gap-1 align-items-center justify-content-between">
                                        <div className="d-flex gap-1">
                                            <h4 className="mb-0">Unused IDs</h4>
                                            <CounterBadge count={unusedIds.length} />
                                        </div>
                                        <Button
                                            color="link"
                                            size="xs"
                                            onClick={unusedIdsActions.removeAll}
                                            className="p-0"
                                            title="Remove all unused IDs from the list"
                                            disabled={unusedIds.length === 0}
                                        >
                                            Remove all
                                        </Button>
                                    </div>
                                    <Alert color="info" className="mt-1">
                                        <Icon icon="info" />
                                        The Unused Database IDs will not be used in the Change Vector generated for a
                                        new or modified document
                                    </Alert>
                                    <UnusedIdsForm
                                        ids={unusedIds}
                                        addId={unusedIdsActions.add}
                                        removeId={unusedIdsActions.remove}
                                    />
                                    <PotentialUnusedIdList
                                        potentialUnusedId={potentialUnusedId}
                                        unusedIds={unusedIds}
                                        unusedIdsActions={unusedIdsActions}
                                    />
                                </div>
                            </CardBody>
                        </LazyLoad>
                    </Card>
                </Col>
                <Col sm={12} lg={4}>
                    <UnusedDatabaseIdsAboutView />
                </Col>
            </Row>
        </div>
    );
}
