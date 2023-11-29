import React, { useState } from "react";
import { Alert, Button, Card, CardBody, Col, Form, Row } from "reactstrap";
import { AboutViewAnchored, AboutViewHeading, AccordionItemWrapper } from "components/common/AboutView";
import ButtonWithSpinner from "components/common/ButtonWithSpinner";
import { Icon } from "components/common/Icon";
import { CounterBadge } from "components/common/CounterBadge";
import UsedIdsPill from "components/pages/database/settings/unusedDatabaseIds/bits/UsedIdsPill";
import { NonShardedViewProps } from "components/models/common";
import UnusedIdsSelect from "components/pages/database/settings/unusedDatabaseIds/bits/UnusedIdsSelect";
import PotentialUnusedIdsPill from "components/pages/database/settings/unusedDatabaseIds/bits/PotentialUnusedIdsPill";
import { todo } from "common/developmentHelper";

todo("Feature", "Damian", "Add missing logic");
todo("Feature", "Damian", "Connect to Studio");
todo("Feature", "Damian", "Remove legacy code");
todo("Other", "Danielle", "Add Info Hub text");

export default function UnusedDatabaseIds({ db }: NonShardedViewProps) {
    const usedIds = [
        { vector: "EPICO8OeJU6pkaOeitge1Q", node: "A", shard: "#1" },
        { vector: "Qd2KFEELSUSOSY7uha618w", node: "DEV", shard: "#6" },
        { vector: "TvKbAdrq5UmUyXdpmlp1kQ", node: "C", shard: "#4" },
    ];

    const unusedIds = [
        { vector: "rH9jpPgORXVKsGCVGHVbzd" },
        { vector: "8zxH3ixETEBuiYbr7Ousg8" },
        { vector: "esBu90ZJUpPLVXCktYszRz" },
        { vector: "aWSiuRNl0NspIj7vHLBTrG" },
    ];

    const [currentUnusedIds, setUnusedIds] = useState(unusedIds);

    const toggleUnusedId = (vector: string) => {
        if (currentUnusedIds.some((id) => id.vector === vector)) {
            setUnusedIds(currentUnusedIds.filter((id) => id.vector !== vector));
        } else {
            setUnusedIds([...currentUnusedIds, { vector }]);
        }
    };

    const removeAllUnusedIds = () => {
        setUnusedIds([]);
    };

    const potentialUnusedIds = [
        { vector: "JLWI6JFUrvGgQAdujNyhBq" },
        { vector: "LT7eFjLXwwPtsqRLSITbbp" },
        { vector: "Ma3sVWpuJbBX3TyBzaToQQ" },
    ];

    const addAllPotentialUnusedIds = () => {
        const newIds = potentialUnusedIds.filter(
            (id) => !currentUnusedIds.some((currentId) => currentId.vector === id.vector)
        );
        setUnusedIds([...currentUnusedIds, ...newIds]);
    };

    return (
        <div className="content-margin">
            <Col xxl={12}>
                <Row className="gy-sm">
                    <Col>
                        <Form autoComplete="off">
                            <AboutViewHeading title="Unused Database IDs" icon="database-id" />
                            <div id="saveUnusedDatabaseIds" className="w-fit-content">
                                <ButtonWithSpinner
                                    type="submit"
                                    color="primary"
                                    className="mb-3"
                                    icon="save"
                                    isSpinning={null}
                                >
                                    Save
                                </ButtonWithSpinner>
                            </div>
                            <Card className="mb-3">
                                <CardBody>
                                    <div className="vstack gap-2">
                                        <div className="d-flex gap-1 align-items-center">
                                            <h4 className="mb-0">Used IDs</h4>
                                            <CounterBadge count={usedIds.length} />
                                        </div>
                                        <div className="used-ids-grid">
                                            {usedIds.map((id, index) => (
                                                <UsedIdsPill
                                                    key={index}
                                                    vector={id.vector}
                                                    node={id.node}
                                                    shard={id.shard}
                                                />
                                            ))}
                                        </div>
                                    </div>
                                </CardBody>
                            </Card>
                            <Card className="mb-3">
                                <CardBody>
                                    <div className="vstack gap-2">
                                        <div className="d-flex gap-1 align-items-center justify-content-between">
                                            <div className="d-flex gap-1">
                                                <h4 className="mb-0">Unused IDs</h4>
                                                <CounterBadge count={currentUnusedIds.length} />
                                            </div>
                                            <Button
                                                color="link"
                                                size="xs"
                                                onClick={removeAllUnusedIds}
                                                className="p-0"
                                                title="Remove all unused IDs from the list"
                                                disabled={currentUnusedIds.length === 0}
                                            >
                                                Remove all
                                            </Button>
                                        </div>
                                        <Alert color="info" className="mt-1">
                                            <Icon icon="info" />
                                            The Unused Database IDs will not be used in the Change Vector generated for
                                            a new or modified document
                                        </Alert>
                                        <UnusedIdsSelect ids={currentUnusedIds} onRemoveId={toggleUnusedId} />
                                        {potentialUnusedIds.length > 0 && (
                                            <>
                                                <div className="d-flex gap-1 align-items-center justify-content-between mt-3">
                                                    <div className="d-flex gap-1">
                                                        <h4 className="mb-0">IDs that may be added to the list</h4>
                                                        <CounterBadge count={potentialUnusedIds.length} />
                                                    </div>
                                                    <Button
                                                        color="link"
                                                        size="xs"
                                                        onClick={addAllPotentialUnusedIds}
                                                        className="p-0"
                                                        title="Add all potential unused IDs to the list"
                                                    >
                                                        Add all
                                                    </Button>
                                                </div>
                                                <div className="d-flex flex-wrap gap-1 mt-1">
                                                    {potentialUnusedIds.map((id, index) => (
                                                        <PotentialUnusedIdsPill
                                                            key={index}
                                                            vector={id.vector}
                                                            onClick={() => toggleUnusedId(id.vector)}
                                                            isAdded={currentUnusedIds.some(
                                                                (item) => item.vector === id.vector
                                                            )}
                                                        />
                                                    ))}
                                                </div>
                                            </>
                                        )}
                                    </div>
                                </CardBody>
                            </Card>
                        </Form>
                    </Col>
                    <Col sm={12} lg={4}>
                        <AboutViewAnchored>
                            <AccordionItemWrapper
                                targetId="about"
                                icon="about"
                                color="info"
                                description="Get additional info on this feature"
                                heading="About this view"
                            >
                                Text for Unused Database IDs
                            </AccordionItemWrapper>
                        </AboutViewAnchored>
                    </Col>
                </Row>
            </Col>
        </div>
    );
}
