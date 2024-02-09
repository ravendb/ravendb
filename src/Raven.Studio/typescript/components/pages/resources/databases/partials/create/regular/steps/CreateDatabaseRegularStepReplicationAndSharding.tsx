import { Switch } from "components/common/Checkbox";
import { FormInput, FormSwitch } from "components/common/Form";
import { Icon } from "components/common/Icon";
import { LicenseRestrictions } from "components/common/LicenseRestrictions";
import { licenseSelectors } from "components/common/shell/licenseSlice";
import { CreateDatabaseRegularFormData } from "../createDatabaseRegularValidation";
import { useAppSelector } from "components/store";
import React from "react";
import { useFormContext, useWatch } from "react-hook-form";
import { Alert, Col, Collapse, InputGroup, InputGroupText, PopoverBody, Row, UncontrolledPopover } from "reactstrap";
import { clusterSelectors } from "components/common/shell/clusterSlice";

const shardingImg = require("Content/img/createDatabase/sharding.svg");

export default function CreateDatabaseRegularStepReplicationAndSharding() {
    const availableNodesCount = useAppSelector(clusterSelectors.allNodes).length;

    // TODO
    // const maxReplicationFactorForSharding = useAppSelector(
    //     licenseSelectors.statusValue("MaxReplicationFactorForSharding")
    // );
    // const hasMultiNodeSharding = useAppSelector(licenseSelectors.statusValue("HasMultiNodeSharding"));

    // TODO remove shard number range input

    const hasDynamicNodesDistribution = useAppSelector(licenseSelectors.statusValue("HasDynamicNodesDistribution"));

    const { control } = useFormContext<CreateDatabaseRegularFormData>();

    const formValues = useWatch({
        control,
    });

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

            <UncontrolledPopover target="ReplicationInfo" placement="top" trigger="hover" container="PopoverContainer">
                <PopoverBody>
                    <div>
                        Add more{" "}
                        <strong className="text-node">
                            <Icon icon="node" margin="m-0" /> Instance nodes
                        </strong>{" "}
                        in <a href="#">Manage cluster</a> view
                    </div>
                </PopoverBody>
            </UncontrolledPopover>

            <UncontrolledPopover target="ShardingInfo" placement="top" trigger="hover" container="PopoverContainer">
                <PopoverBody>
                    <p>
                        <strong className="text-shard">
                            <Icon icon="sharding" margin="m-0" /> Sharding
                        </strong>{" "}
                        is a database partitioning technique that breaks up large databases into smaller, more
                        manageable pieces called{" "}
                        <strong className="text-shard">
                            {" "}
                            <Icon icon="shard" margin="m-0" />
                            shards
                        </strong>
                        .
                    </p>
                    <p>
                        Each shard contains a subset of the data and can be stored on a separate server, allowing for{" "}
                        <strong>horizontal scalability and improved performance</strong>.
                    </p>
                    <a href="#">
                        Learn more TODO <Icon icon="newtab" margin="m-0" />
                    </a>
                </PopoverBody>
            </UncontrolledPopover>
            <Row>
                <Col lg={{ offset: 2, size: 8 }}>
                    <Row className="pt-2">
                        <span id="ReplicationInfo">
                            <Icon icon="info" color="info" margin="m-0" /> Available nodes:{" "}
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
                        <span id="ShardingInfo">
                            <Icon icon="info" color="info" margin="m-0" /> What is sharding?
                        </span>
                    </Row>
                    <Row className="pt-2">
                        <Col>
                            <LicenseRestrictions
                                isAvailable={true} // TODO
                                featureName={
                                    <strong className="text-shard">
                                        <Icon icon="sharding" margin="m-0" /> Sharding
                                    </strong>
                                }
                                className="d-inline-block"
                            >
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
                            </LicenseRestrictions>
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
                    {hasDynamicNodesDistribution ? (
                        <LicenseRestrictions
                            isAvailable={formValues.replicationAndSharding.replicationFactor > 1}
                            message="Replication factor is set to 1"
                            className="d-inline-block"
                        >
                            <FormSwitch
                                control={control}
                                name="replicationAndSharding.isDynamicDistribution"
                                color="primary"
                                disabled={formValues.replicationAndSharding.replicationFactor <= 1}
                            >
                                Allow dynamic database distribution
                                <br />
                                <small className="text-muted">Maintain replication factor upon node failure</small>
                            </FormSwitch>
                        </LicenseRestrictions>
                    ) : (
                        <LicenseRestrictions
                            isAvailable={hasDynamicNodesDistribution}
                            featureName="dynamic database distribution"
                            className="d-inline-block"
                        >
                            <Switch
                                color="primary"
                                selected={false}
                                toggleSelection={null}
                                disabled={hasDynamicNodesDistribution}
                            >
                                Allow dynamic database distribution
                                <br />
                                <small className="text-muted">Maintain replication factor upon node failure</small>
                            </Switch>
                        </LicenseRestrictions>
                    )}
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
