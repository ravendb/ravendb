import { FormInput, FormRange, FormSwitch } from "components/common/Form";
import { Icon } from "components/common/Icon";
import { CreateDatabaseRegularFormData } from "../createDatabaseRegularValidation";
import { useAppSelector } from "components/store";
import { useEffect } from "react";
import { useFormContext, useWatch } from "react-hook-form";
import Collapse from "react-bootstrap/Collapse";
import InputGroup from "react-bootstrap/InputGroup";
import Row from "react-bootstrap/Row";
import Col from "react-bootstrap/Col";
import { clusterSelectors } from "components/common/shell/clusterSlice";
import { licenseSelectors } from "components/common/shell/licenseSlice";
import { LicenseRestrictedMessage } from "components/common/LicenseRestrictedMessage";
import { ConditionalPopover } from "components/common/ConditionalPopover";
import { useAppUrls } from "components/hooks/useAppUrls";
import { useRavenLink } from "components/hooks/useRavenLink";
import classNames from "classnames";
import { createDatabaseRegularDataUtils } from "components/pages/resources/databases/partials/create/regular/createDatabaseRegularDataUtils";
import RichAlert from "components/common/RichAlert";
import PopoverWithHoverWrapper from "components/common/PopoverWithHoverWrapper";

const shardingImg = require("Content/img/createDatabase/sharding.svg");

export default function CreateDatabaseRegularStepReplicationAndSharding() {
    const hasDynamicNodesDistribution = useAppSelector(licenseSelectors.statusValue("HasDynamicNodesDistribution"));
    const maxReplicationFactorForSharding =
        useAppSelector(licenseSelectors.statusValue("MaxReplicationFactorForSharding")) ?? Infinity;

    const { appUrl } = useAppUrls();
    const docsShardingLink = useRavenLink({
        hash: "VKF52P",
    });

    const { control, setValue, watch } = useFormContext<CreateDatabaseRegularFormData>();
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

    // Disable prefixes for shards when sharding is disabled
    useEffect(() => {
        const { unsubscribe } = watch((values, { name }) => {
            if (name === "replicationAndShardingStep.isSharded" && !values.replicationAndShardingStep.isSharded) {
                setValue("replicationAndShardingStep.isPrefixesForShards", false, { shouldValidate: true });
            }
        });

        return () => unsubscribe();
    }, [setValue, watch]);

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
                <Col lg={{ span: 8, offset: 2 }} className="text-center">
                    <p>
                        Database replication provides benefits such as improved data availability, increased
                        scalability, and enhanced disaster recovery capabilities.
                    </p>
                    <p>
                        <span>
                            <PopoverWithHoverWrapper
                                message={
                                    <>
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
                                            Each shard contains a subset of the data and can be stored on a separate
                                            server, allowing for{" "}
                                            <strong>horizontal scalability and improved performance</strong>.
                                        </p>
                                        <a href={docsShardingLink}>
                                            Learn more <Icon icon="newtab" margin="m-0" />
                                        </a>
                                    </>
                                }
                            >
                                <Icon icon="info" color="info" margin="m-0" />
                            </PopoverWithHoverWrapper>{" "}
                            What is sharding?
                        </span>
                    </p>
                </Col>
            </Row>

            <Row>
                <Col lg={{ offset: 1, span: 10 }}>
                    <Row className="pt-2">
                        <Col sm="6" className="d-flex gap-1 align-items-center">
                            <PopoverWithHoverWrapper
                                message={
                                    <>
                                        Add more{" "}
                                        <strong className="text-node">
                                            <Icon icon="node" margin="m-0" /> Instance nodes
                                        </strong>{" "}
                                        in <a href={appUrl.forCluster()}>Manage cluster</a> view
                                    </>
                                }
                            >
                                <Icon id="ReplicationInfo" icon="info" color="info" margin="m-0" />
                            </PopoverWithHoverWrapper>{" "}
                            Available nodes: <Icon icon="node" color="node" margin="ms-1" />{" "}
                            <strong className={classNames({ "text-warning": isReplicationFactorWarning })}>
                                {maxReplicationFactor}{" "}
                                {isReplicationFactorWarning && (
                                    <PopoverWithHoverWrapper
                                        message={
                                            <LicenseRestrictedMessage>
                                                Your license doesn&apos;t allow replication factor higher than{" "}
                                                <strong>{maxReplicationFactorForSharding}</strong> for sharded database.
                                            </LicenseRestrictedMessage>
                                        }
                                    >
                                        <Icon icon="warning" margin="m-0" />
                                    </PopoverWithHoverWrapper>
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
                            <Collapse in={isReplicationFactorVisible}>
                                <div>
                                    <InputGroup>
                                        <InputGroup.Text>Replication Factor</InputGroup.Text>
                                        <FormInput
                                            type="number"
                                            control={control}
                                            name="replicationAndShardingStep.replicationFactor"
                                            className="replication-input"
                                            min="1"
                                            max={maxReplicationFactor}
                                        />
                                    </InputGroup>
                                    <FormRange
                                        control={control}
                                        name="replicationAndShardingStep.replicationFactor"
                                        min="1"
                                        max={maxReplicationFactor}
                                        className="mt-3"
                                    />
                                </div>
                            </Collapse>
                        </Col>
                        <Col sm="6">
                            <Collapse in={isSharded}>
                                <div>
                                    <InputGroup>
                                        <InputGroup.Text>Number of shards</InputGroup.Text>
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
                                </div>
                            </Collapse>
                        </Col>
                    </Row>
                    <RichAlert variant="info" className="mt-4">
                        <Collapse in={isSharded}>
                            <div>
                                Data will be divided into{" "}
                                <strong>
                                    {shardsCount}
                                    <Icon icon="shard" margin="m-0" /> Shards
                                </strong>
                                .<br />
                            </div>
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
                    </RichAlert>
                </Col>
            </Row>

            <Row className="mt-4">
                <Col lg={{ offset: 1, span: 5 }}>
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
                <Col lg={{ span: 5 }}>
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
