import { FormInput, FormSwitch } from "components/common/Form";
import { Icon } from "components/common/Icon";
import { CreateDatabaseRegularFormData } from "../createDatabaseRegularValidation";
import { useAppSelector } from "components/store";
import React, { useEffect } from "react";
import { useFormContext, useWatch } from "react-hook-form";
import { Alert, Col, Collapse, InputGroup, InputGroupText, PopoverBody, Row, UncontrolledPopover } from "reactstrap";
import { clusterSelectors } from "components/common/shell/clusterSlice";
import { licenseSelectors } from "components/common/shell/licenseSlice";
import { LicenseRestrictedMessage } from "components/common/LicenseRestrictedMessage";
import { ConditionalPopover } from "components/common/ConditionalPopover";
import { useAppUrls } from "components/hooks/useAppUrls";

const shardingImg = require("Content/img/createDatabase/sharding.svg");

export default function CreateDatabaseRegularStepReplicationAndSharding() {
    const hasDynamicNodesDistribution = useAppSelector(licenseSelectors.statusValue("HasDynamicNodesDistribution"));
    const maxReplicationFactorForSharding =
        useAppSelector(licenseSelectors.statusValue("MaxReplicationFactorForSharding")) ?? Infinity;

    const { control, setValue } = useFormContext<CreateDatabaseRegularFormData>();
    const {
        replicationAndSharding: { isSharded, shardsCount, replicationFactor, isManualReplication },
    } = useWatch({
        control,
    });

    const availableNodesCount = useAppSelector(clusterSelectors.allNodes).length;

    const maxReplicationFactor = isSharded
        ? Math.min(maxReplicationFactorForSharding, availableNodesCount)
        : availableNodesCount;

    const isReplicationFactorDisabled = isManualReplication && !isSharded;

    useEffect(() => {
        if (isSharded && replicationFactor > maxReplicationFactorForSharding) {
            setValue("replicationAndSharding.replicationFactor", maxReplicationFactorForSharding);
        }
    }, [replicationFactor, isSharded, maxReplicationFactorForSharding, setValue]);

    const { appUrl } = useAppUrls();

    return (
        <div>
            <div className="d-flex justify-content-center">
                <img src={shardingImg} alt="Sharding" className="step-img" />
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
                            <UncontrolledPopover
                                target="ReplicationInfo"
                                placement="left"
                                trigger="hover"
                                className="bs5"
                            >
                                {isSharded && maxReplicationFactorForSharding < availableNodesCount ? (
                                    <PopoverBody>
                                        <LicenseRestrictedMessage>
                                            Your license doesn&apos;t allow replication factor higher than{" "}
                                            {maxReplicationFactorForSharding} for sharded database.
                                        </LicenseRestrictedMessage>
                                    </PopoverBody>
                                ) : (
                                    <PopoverBody>
                                        Add more{" "}
                                        <strong className="text-node">
                                            <Icon icon="node" margin="m-0" /> Instance nodes
                                        </strong>{" "}
                                        in <a href={appUrl.forCluster()}>Manage cluster</a> view
                                    </PopoverBody>
                                )}
                            </UncontrolledPopover>
                            <Icon icon="node" color="node" margin="ms-1" /> <strong>{maxReplicationFactor}</strong>
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
                                    max={maxReplicationFactor}
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
                                max={maxReplicationFactor}
                                className="mt-1"
                                disabled={isReplicationFactorDisabled}
                            />
                        </Col>
                    </Row>
                    <Row className="pt-2">
                        <span>
                            <Icon id="ShardingInfo" icon="info" color="info" margin="m-0" /> What is sharding?
                            <UncontrolledPopover target="ShardingInfo" placement="left" trigger="hover" className="bs5">
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
                                    <a href="#TODO">
                                        Learn more <Icon icon="newtab" margin="m-0" />
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
                            <Collapse isOpen={isSharded}>
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
                        <Collapse isOpen={isSharded}>
                            <>
                                Data will be divided into{" "}
                                <strong>
                                    {shardsCount}
                                    <Icon icon="shard" margin="m-0" /> Shards
                                </strong>
                                .<br />
                            </>
                        </Collapse>
                        {replicationFactor > 1 ? (
                            <>
                                {isSharded ? <>Each shard</> : <>Data</>} will be replicated to{" "}
                                <strong>
                                    {replicationFactor} <Icon icon="node" margin="m-0" /> Nodes
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
                    <ConditionalPopover
                        conditions={{
                            isActive: !hasDynamicNodesDistribution,
                            message: (
                                <LicenseRestrictedMessage>
                                    Current license doesn&apos;t include{" "}
                                    <strong>Dynamic database distribution feature</strong>.
                                </LicenseRestrictedMessage>
                            ),
                        }}
                    >
                        <FormSwitch
                            control={control}
                            name="replicationAndSharding.isDynamicDistribution"
                            color="primary"
                            disabled={!hasDynamicNodesDistribution}
                        >
                            Allow dynamic database distribution
                            <br />
                            <small className="text-muted">Maintain replication factor upon node failure</small>
                        </FormSwitch>
                    </ConditionalPopover>
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