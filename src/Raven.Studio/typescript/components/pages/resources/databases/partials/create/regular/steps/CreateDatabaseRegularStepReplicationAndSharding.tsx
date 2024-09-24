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
import { useRavenLink } from "components/hooks/useRavenLink";
import classNames from "classnames";
import { createDatabaseRegularDataUtils } from "components/pages/resources/databases/partials/create/regular/createDatabaseRegularDataUtils";

const shardingImg = require("Content/img/createDatabase/sharding.svg");

export default function CreateDatabaseRegularStepReplicationAndSharding() {
    const hasDynamicNodesDistribution = useAppSelector(licenseSelectors.statusValue("HasDynamicNodesDistribution"));
    const maxReplicationFactorForSharding =
        useAppSelector(licenseSelectors.statusValue("MaxReplicationFactorForSharding")) ?? Infinity;

    const { appUrl } = useAppUrls();
    const docsShardingLink = useRavenLink({
        hash: "VKF52P",
    });

    const { control, setValue } = useFormContext<CreateDatabaseRegularFormData>();
    const {
        basicInfoStep: { isEncrypted },
        replicationAndShardingStep: { isSharded, shardsCount, replicationFactor, isManualReplication },
    } = useWatch({
        control,
    });

    const nodeTagsCount = useAppSelector(clusterSelectors.allNodes).length;
    const availableNodesCount = nodeTagsCount || 1;

    const maxReplicationFactor = isSharded
        ? Math.min(maxReplicationFactorForSharding, availableNodesCount)
        : availableNodesCount;

    const isReplicationFactorVisible = !isManualReplication || isSharded;
    const isReplicationFactorWarning = isSharded && maxReplicationFactorForSharding < availableNodesCount;

    const isNotBootstrapped = nodeTagsCount === 0;
    const isManualReplicationRequiredForEncryption =
        createDatabaseRegularDataUtils.getIsManualReplicationRequiredForEncryption(nodeTagsCount, isEncrypted);

    useEffect(() => {
        if (isSharded && replicationFactor > maxReplicationFactorForSharding) {
            setValue("replicationAndShardingStep.replicationFactor", maxReplicationFactorForSharding);
        }
    }, [replicationFactor, isSharded, maxReplicationFactorForSharding, setValue]);

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
                    <p>
                        <span>
                            <Icon id="ShardingInfo" icon="info" color="info" margin="m-0" /> What is sharding?
                            <UncontrolledPopover target="ShardingInfo" placement="top" trigger="hover" className="bs5">
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
                                    <a href={docsShardingLink}>
                                        Learn more <Icon icon="newtab" margin="m-0" />
                                    </a>
                                </PopoverBody>
                            </UncontrolledPopover>
                        </span>
                    </p>
                </Col>
            </Row>

            <Row>
                <Col lg={{ offset: 1, size: 10 }}>
                    <Row className="pt-2">
                        <Col sm="6" className="d-flex gap-1 align-items-center">
                            <Icon id="ReplicationInfo" icon="info" color="info" margin="m-0" /> Available nodes:{" "}
                            <UncontrolledPopover
                                target="ReplicationInfo"
                                placement="right"
                                trigger="hover"
                                className="bs5"
                            >
                                <PopoverBody>
                                    Add more{" "}
                                    <strong className="text-node">
                                        <Icon icon="node" margin="m-0" /> Instance nodes
                                    </strong>{" "}
                                    in <a href={appUrl.forCluster()}>Manage cluster</a> view
                                </PopoverBody>
                            </UncontrolledPopover>
                            <Icon icon="node" color="node" margin="ms-1" />{" "}
                            <strong className={classNames({ "text-warning": isReplicationFactorWarning })}>
                                {maxReplicationFactor}{" "}
                                {isReplicationFactorWarning && (
                                    <>
                                        <Icon id="LicenseWarning" icon="warning" margin="m-0" />
                                        <UncontrolledPopover
                                            target="LicenseWarning"
                                            placement="right"
                                            trigger="hover"
                                            className="bs5"
                                        >
                                            <PopoverBody>
                                                <LicenseRestrictedMessage>
                                                    Your license doesn&apos;t allow replication factor higher than{" "}
                                                    <strong>{maxReplicationFactorForSharding}</strong> for sharded
                                                    database.
                                                </LicenseRestrictedMessage>
                                            </PopoverBody>
                                        </UncontrolledPopover>
                                    </>
                                )}
                            </strong>
                        </Col>
                        <Col sm="6">
                            <FormSwitch
                                control={control}
                                name="replicationAndShardingStep.isSharded"
                                color="shard"
                                className="mt-1"
                            >
                                Enable{" "}
                                <strong className="text-shard">
                                    <Icon icon="sharding" margin="m-0" /> Sharding
                                </strong>
                            </FormSwitch>
                        </Col>
                    </Row>
                    <Row className="pt-2">
                        <Col sm="6">
                            <Collapse isOpen={isReplicationFactorVisible}>
                                <InputGroup>
                                    <InputGroupText>Replication Factor</InputGroupText>
                                    <FormInput
                                        type="number"
                                        control={control}
                                        name="replicationAndShardingStep.replicationFactor"
                                        className="replication-input"
                                        min="1"
                                        max={maxReplicationFactor}
                                    />
                                </InputGroup>
                                <FormInput
                                    type="range"
                                    control={control}
                                    name="replicationAndShardingStep.replicationFactor"
                                    min="1"
                                    max={maxReplicationFactor}
                                    className="mt-3"
                                />
                            </Collapse>
                        </Col>
                        <Col sm="6">
                            <Collapse isOpen={isSharded}>
                                <InputGroup>
                                    <InputGroupText>Number of shards</InputGroupText>
                                    <FormInput
                                        type="number"
                                        control={control}
                                        name="replicationAndShardingStep.shardsCount"
                                        className="replication-input"
                                        min="1"
                                        max="100"
                                    />
                                </InputGroup>
                                <FormSwitch
                                    control={control}
                                    name="replicationAndShardingStep.isPrefixesForShards"
                                    color="primary"
                                    className="mt-3"
                                >
                                    Add <strong>prefixes</strong> for shards
                                    <br />
                                    <small className="text-muted">
                                        Manage document distribution by defining
                                        <br />a prefix for document IDs
                                    </small>
                                </FormSwitch>
                            </Collapse>
                        </Col>
                    </Row>
                    <Alert color="info" className="text-center mt-4">
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
                                {isSharded ? <>Each shard</> : <>Data</>} will be replicated across{" "}
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

            <Row className="mt-4">
                <Col lg={{ offset: 1, size: 5 }}>
                    <ConditionalPopover
                        conditions={{
                            isActive: !hasDynamicNodesDistribution,
                            message: (
                                <LicenseRestrictedMessage>
                                    Current license doesn&apos;t include{" "}
                                    <strong className="text-info">Dynamic database distribution feature</strong>.
                                </LicenseRestrictedMessage>
                            ),
                        }}
                        popoverPlacement="top"
                    >
                        <FormSwitch
                            control={control}
                            name="replicationAndShardingStep.isDynamicDistribution"
                            color="primary"
                            disabled={!hasDynamicNodesDistribution}
                        >
                            Allow dynamic database distribution
                            <br />
                            <small className="text-muted">Maintain replication factor upon node failure</small>
                        </FormSwitch>
                    </ConditionalPopover>
                </Col>
                <Col lg={{ size: 5 }}>
                    <ConditionalPopover
                        conditions={[
                            {
                                isActive: isNotBootstrapped,
                                message: (
                                    <span>
                                        Please, first <a href={appUrl.forCluster()}>Bootstrap a Cluster</a>.
                                    </span>
                                ),
                            },
                            {
                                isActive: isManualReplicationRequiredForEncryption,
                                message:
                                    "You need to select nodes manually because the encryption is enabled and there are more than 1 nodes in cluster.",
                            },
                        ]}
                        popoverPlacement="top"
                    >
                        <FormSwitch
                            control={control}
                            name="replicationAndShardingStep.isManualReplication"
                            color="primary"
                            disabled={isNotBootstrapped || isManualReplicationRequiredForEncryption}
                        >
                            Set replication nodes manually
                            <br />
                            <small className="text-muted">Select nodes from the list in the next step</small>
                        </FormSwitch>
                    </ConditionalPopover>
                </Col>
            </Row>
        </div>
    );
}
