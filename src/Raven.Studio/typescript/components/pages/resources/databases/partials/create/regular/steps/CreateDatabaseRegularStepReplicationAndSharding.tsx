import { FormInput, FormSwitch } from "components/common/Form";
import { Icon } from "components/common/Icon";
import { CreateDatabaseRegularFormData } from "../createDatabaseRegularValidation";
import { useAppSelector } from "components/store";
import React from "react";
import { useFormContext, useWatch } from "react-hook-form";
import { Alert, Col, Collapse, InputGroup, InputGroupText, PopoverBody, Row, UncontrolledPopover } from "reactstrap";
import { clusterSelectors } from "components/common/shell/clusterSlice";

const shardingImg = require("Content/img/createDatabase/sharding.svg");

export default function CreateDatabaseRegularStepReplicationAndSharding() {
    const { control } = useFormContext<CreateDatabaseRegularFormData>();
    const formValues = useWatch({
        control,
    });

    const availableNodesCount = useAppSelector(clusterSelectors.allNodes).length;

    const isReplicationFactorDisabled =
        formValues.replicationAndSharding.isManualReplication && !formValues.replicationAndSharding.isSharded;

    return (
        <div>
            <div className="d-flex justify-content-center">
                <img src={shardingImg} alt="" className="step-img" />
            </div>

            <h2 className="text-center">Replication & Sharding</h2>

            <Row>
                <Col lg={{ size: 8, offset: 2 }} className="text-center">
                    <p>
                        Database replication provides benefits such as improved data availability, increased
                        scalability, and enhanced disaster recovery capabilities.
                    </p>
                </Col>
            </Row>

            <Row>
                <Col lg={{ offset: 2, size: 8 }}>
                    <Row className="pt-2">
                        <span>
                            <Icon id="ReplicationInfo" icon="info" color="info" margin="m-0" /> Available nodes:{" "}
                            <UncontrolledPopover target="ReplicationInfo" placement="left" trigger="hover">
                                <PopoverBody>
                                    Add more{" "}
                                    <strong className="text-node">
                                        <Icon icon="node" margin="m-0" /> Instance nodes
                                    </strong>{" "}
                                    in <a href="#">Manage cluster</a> view
                                </PopoverBody>
                            </UncontrolledPopover>
                            <Icon icon="node" color="node" margin="ms-1" /> <strong>{availableNodesCount}</strong>
                        </span>
                    </Row>
                    <Row className="pt-2">
                        <Col sm="auto">
                            <InputGroup>
                                <InputGroupText>Replication Factor</InputGroupText>
                                <FormInput
                                    type="number"
                                    control={control}
                                    name="replicationAndSharding.replicationFactor"
                                    className="replication-input"
                                    min="1"
                                    max={availableNodesCount}
                                    disabled={isReplicationFactorDisabled}
                                />
                            </InputGroup>
                        </Col>
                        <Col>
                            <FormInput
                                type="range"
                                control={control}
                                name="replicationAndSharding.replicationFactor"
                                min="1"
                                max={availableNodesCount}
                                className="mt-1"
                                disabled={isReplicationFactorDisabled}
                            />
                        </Col>
                    </Row>
                    <Row className="pt-2">
                        <span>
                            <Icon id="ShardingInfo" icon="info" color="info" margin="m-0" /> What is sharding?
                            <UncontrolledPopover target="ShardingInfo" placement="left" trigger="hover">
                                <PopoverBody>
                                    <p>
                                        <strong className="text-shard">
                                            <Icon icon="sharding" margin="m-0" /> Sharding
                                        </strong>{" "}
                                        is a database partitioning technique that breaks up large databases into
                                        smaller, more manageable pieces called{" "}
                                        <strong className="text-shard">
                                            {" "}
                                            <Icon icon="shard" margin="m-0" />
                                            shards
                                        </strong>
                                        .
                                    </p>
                                    <p>
                                        Each shard contains a subset of the data and can be stored on a separate server,
                                        allowing for <strong>horizontal scalability and improved performance</strong>.
                                    </p>
                                    <a href="#">
                                        Learn more TODO <Icon icon="newtab" margin="m-0" />
                                    </a>
                                </PopoverBody>
                            </UncontrolledPopover>
                        </span>
                    </Row>
                    <Row className="pt-2">
                        <Col>
                            <FormSwitch
                                control={control}
                                name="replicationAndSharding.isSharded"
                                color="shard"
                                className="mt-1"
                            >
                                Enable{" "}
                                <strong className="text-shard">
                                    <Icon icon="sharding" margin="m-0" /> Sharding
                                </strong>
                            </FormSwitch>
                        </Col>
                        <Col sm="auto">
                            <Collapse isOpen={formValues.replicationAndSharding.isSharded}>
                                <InputGroup>
                                    <InputGroupText>Number of shards</InputGroupText>
                                    <FormInput
                                        type="number"
                                        control={control}
                                        name="replicationAndSharding.shardsCount"
                                        className="replication-input"
                                    />
                                </InputGroup>
                            </Collapse>
                        </Col>
                    </Row>
                    <Alert color="info" className="text-center mt-2">
                        <Collapse isOpen={formValues.replicationAndSharding.isSharded}>
                            <>
                                Data will be divided into{" "}
                                <strong>
                                    {formValues.replicationAndSharding.shardsCount}
                                    <Icon icon="shard" margin="m-0" /> Shards
                                </strong>
                                .<br />
                            </>
                        </Collapse>
                        {formValues.replicationAndSharding.replicationFactor > 1 ? (
                            <>
                                {formValues.replicationAndSharding.isSharded ? <>Each shard</> : <>Data</>} will be
                                replicated to{" "}
                                <strong>
                                    {formValues.replicationAndSharding.replicationFactor}{" "}
                                    <Icon icon="node" margin="m-0" /> Nodes
                                </strong>
                                .
                            </>
                        ) : (
                            <>Data won&apos;t be replicated.</>
                        )}
                    </Alert>
                </Col>
            </Row>

            <Row className="mt-2">
                <Col>
                    <FormSwitch control={control} name="replicationAndSharding.isDynamicDistribution" color="primary">
                        Allow dynamic database distribution
                        <br />
                        <small className="text-muted">Maintain replication factor upon node failure</small>
                    </FormSwitch>
                </Col>
                <Col>
                    <FormSwitch control={control} name="replicationAndSharding.isManualReplication" color="primary">
                        Set replication nodes manually
                        <br />
                        <small className="text-muted">Select nodes from the list in the next step</small>
                    </FormSwitch>
                </Col>
            </Row>
        </div>
    );
}

// const shardedFieldId = "sharded";
// const dynamicDistributionFieldId = "dynamic-distribution";

// function ShardingUnavailablePopover() {
//     return (
//         <UncontrolledPopover target={shardedFieldId} placement="left" trigger="hover">
//             <PopoverBody>
//                 <LicenseRestrictedMessage
//                     featureName={
//                         <strong className="text-primary">
//                             <Icon icon="sharding" />
//                             Sharding
//                         </strong>
//                     }
//                 />
//             </PopoverBody>
//         </UncontrolledPopover>
//     );
// }

// function getDynamicDistributionDisabledPopover(hasDynamicNodesDistribution: boolean, replicationFactor: number) {
//     if (!hasDynamicNodesDistribution) {
//         return <DynamicNodesDistributionUnavailablePopover />;
//     }

//     if (replicationFactor === 1) {
//         return <ReplicationFactorPopover />;
//     }

//     return null;
// }

// function DynamicNodesDistributionUnavailablePopover() {
//     return (
//         <UncontrolledPopover target={dynamicDistributionFieldId} placement="left" trigger="hover">
//             <PopoverBody>
//                 <LicenseRestrictedMessage featureName="dynamic database distribution" />
//             </PopoverBody>
//         </UncontrolledPopover>
//     );
// }

// function ReplicationFactorPopover() {
//     return (
//         <UncontrolledPopover target={dynamicDistributionFieldId} placement="left" trigger="hover">
//             <PopoverBody>Replication factor is set to 1</PopoverBody>
//         </UncontrolledPopover>
//     );
// }
