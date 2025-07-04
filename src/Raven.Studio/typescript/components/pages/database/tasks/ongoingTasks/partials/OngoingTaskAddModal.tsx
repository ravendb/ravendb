import { CounterBadge } from "components/common/CounterBadge";
import { HrHeader } from "components/common/HrHeader";
import { ReactNode } from "react";
import Row from "react-bootstrap/Row";
import Col from "react-bootstrap/Col";
import { Icon } from "components/common/Icon";
import classNames from "classnames";
import { useAppUrls } from "components/hooks/useAppUrls";
import { useAppSelector } from "components/store";
import { useEventsCollector } from "components/hooks/useEventsCollector";
import { licenseSelectors } from "components/common/shell/licenseSlice";
import { getLicenseLimitReachStatus } from "components/utils/licenseLimitsUtils";
import LicenseRestrictedBadge from "components/common/LicenseRestrictedBadge";
import { databaseSelectors } from "components/common/shell/databaseSliceSelectors";
import OverlayTrigger from "react-bootstrap/OverlayTrigger";
import Tooltip from "react-bootstrap/Tooltip";
import Modal from "components/common/Modal";

interface OngoingTaskAddModalProps {
    subscriptionsDatabaseCount: number;
    toggle: () => void;
    isAiOnly?: boolean;
}

export default function OngoingTaskAddModal(props: OngoingTaskAddModalProps) {
    const { toggle, subscriptionsDatabaseCount, isAiOnly } = props;

    const db = useAppSelector(databaseSelectors.activeDatabase);
    const isSharded = db.isSharded;

    const isProfessionalOrAbove = useAppSelector(licenseSelectors.isProfessionalOrAbove);
    const hasExternalReplication = useAppSelector(licenseSelectors.statusValue("HasExternalReplication"));
    const hasReplicationHub = useAppSelector(licenseSelectors.statusValue("HasPullReplicationAsHub"));
    const hasReplicationSink = useAppSelector(licenseSelectors.statusValue("HasPullReplicationAsSink"));
    const hasRavenDbEtl = useAppSelector(licenseSelectors.statusValue("HasRavenEtl"));
    const hasElasticSearchEtl = useAppSelector(licenseSelectors.statusValue("HasElasticSearchEtl"));
    const hasKafkaEtl = useAppSelector(licenseSelectors.statusValue("HasQueueEtl"));
    const hasSqlEtl = useAppSelector(licenseSelectors.statusValue("HasSqlEtl"));
    const hasSnowflakeEtl = useAppSelector(licenseSelectors.statusValue("HasSnowflakeEtl"));
    const hasOlapEtl = useAppSelector(licenseSelectors.statusValue("HasOlapEtl"));
    const hasRabbitMqEtl = useAppSelector(licenseSelectors.statusValue("HasQueueEtl"));
    const hasAzureQueueStorageEtl = useAppSelector(licenseSelectors.statusValue("HasQueueEtl"));
    const hasAmazonSqsEtl = useAppSelector(licenseSelectors.statusValue("HasQueueEtl"));
    const hasKafkaSink = useAppSelector(licenseSelectors.statusValue("HasQueueSink"));
    const hasRabbitMqSink = useAppSelector(licenseSelectors.statusValue("HasQueueSink"));
    const hasPeriodicBackups = useAppSelector(licenseSelectors.statusValue("HasPeriodicBackup"));
    const hasGenAi = useAppSelector(licenseSelectors.statusValue("HasGenAi"));

    const { appUrl } = useAppUrls();

    const subscriptionsServerCount = useAppSelector(licenseSelectors.limitsUsage).NumberOfSubscriptionsInCluster;

    const subscriptionsServerLimit = useAppSelector(licenseSelectors.statusValue("MaxNumberOfSubscriptionsPerCluster"));
    const subscriptionsDatabaseLimit = useAppSelector(
        licenseSelectors.statusValue("MaxNumberOfSubscriptionsPerDatabase")
    );

    const subscriptionsServerLimitStatus = getLicenseLimitReachStatus(
        subscriptionsServerCount,
        subscriptionsServerLimit
    );

    const subscriptionsDatabaseLimitStatus = getLicenseLimitReachStatus(
        subscriptionsDatabaseCount,
        subscriptionsDatabaseLimit
    );

    const getDisableReasonForSharded = (): string => {
        if (!isSharded) {
            return null;
        }

        return "Not supported in sharded databases";
    };

    const isSubscriptionDisabled =
        !isProfessionalOrAbove &&
        (subscriptionsServerLimitStatus === "limitReached" || subscriptionsDatabaseLimitStatus === "limitReached");

    const getSubscriptionDisableReason = (): string => {
        if (!isSubscriptionDisabled) {
            return null;
        }

        const limitReachedReason = subscriptionsServerLimitStatus === "limitReached" ? "Cluster" : "Database";

        return `${limitReachedReason} has reached the maximum number of subscriptions allowed per ${limitReachedReason.toLowerCase()}.`;
    };

    return (
        <Modal
            show
            scrollable
            onHide={toggle}
            contentClassName="modal-border destination-modal bulge-primary"
            size="lg"
        >
            <Modal.Header className="hstack gap-3 pb-0" onCloseClick={toggle}>
                <div className="text-center">
                    <Icon icon="ongoing-tasks" color="primary" addon="plus" className="fs-1" margin="m-0" />
                </div>
                <div className="text-center lead">Add a Database Task</div>
            </Modal.Header>
            <Modal.Body>
                <HrHeader>AI</HrHeader>
                <Row className="gy-sm">
                    <TaskItem
                        title="Create new GenAI task"
                        href={appUrl.forEditGenAi(db.name)}
                        className="ai-etl"
                        target="GenAi"
                        disabled={isSharded}
                        disableReason={getDisableReasonForSharded()}
                    >
                        <Icon icon="genai" margin="m-0" />
                        <h4 className="mt-1 mb-0">GenAI</h4>
                        {!hasGenAi && <LicenseRestrictedBadge licenseRequired="Enterprise AI" />}
                    </TaskItem>
                    <TaskItem
                        title="Create new AI task"
                        href={appUrl.forEditEmbeddingsGeneration(db.name)}
                        className="ai-etl"
                        target="AiETL"
                        disabled={isSharded}
                        disableReason={getDisableReasonForSharded()}
                    >
                        <Icon icon="ai-etl" margin="m-0" />
                        <h4 className="mt-1 mb-0">Embeddings Generation</h4>
                    </TaskItem>
                </Row>

                {!isAiOnly && (
                    <>
                        <HrHeader>Replication</HrHeader>
                        <Row className="gy-sm">
                            <TaskItem
                                title="Create new External Replication task"
                                href={appUrl.forEditExternalReplication(db.name)}
                                className="external-replication"
                                target="ExternalReplication"
                            >
                                <Icon icon="external-replication" />
                                <h4 className="mt-1 mb-0">External Replication</h4>
                                {!hasExternalReplication && <LicenseRestrictedBadge licenseRequired="Professional +" />}
                            </TaskItem>

                            <TaskItem
                                title="Create new Replication Hub task"
                                href={appUrl.forEditReplicationHub(db.name)}
                                className="pull-replication-hub"
                                target="ReplicationHub"
                                disabled={isSharded}
                                disableReason={getDisableReasonForSharded()}
                            >
                                <Icon icon="pull-replication-hub" />
                                <h4 className="mt-1 mb-0">Replication Hub</h4>
                                {!hasReplicationHub && <LicenseRestrictedBadge licenseRequired="Enterprise" />}
                            </TaskItem>
                            <TaskItem
                                title="Create new Replication Sink task"
                                href={appUrl.forEditReplicationSink(db.name)}
                                className="pull-replication-sink"
                                target="ReplicationSink"
                                disabled={isSharded}
                                disableReason={getDisableReasonForSharded()}
                            >
                                <Icon icon="pull-replication-agent" />
                                <h4 className="mt-1 mb-0">Replication Sink</h4>
                                {!hasReplicationSink && <LicenseRestrictedBadge licenseRequired="Professional +" />}
                            </TaskItem>
                        </Row>
                        <HrHeader>Backups & Subscriptions</HrHeader>
                        <Row className="gy-sm">
                            <TaskItem
                                title="Create new Backup task"
                                href={appUrl.forEditPeriodicBackupTask(db.name, "OngoingTasks", false)}
                                className="backup"
                                target="PeriodicBackup"
                            >
                                <Icon icon="periodic-backup" />
                                <h4 className="mt-1 mb-0">Periodic Backup</h4>
                                {!hasPeriodicBackups && <LicenseRestrictedBadge licenseRequired="Professional +" />}
                            </TaskItem>

                            <TaskItem
                                title="Create new Subscription task"
                                href={appUrl.forEditSubscription(db.name)}
                                className="subscription"
                                target="Subscription"
                                disabled={isSubscriptionDisabled}
                                disableReason={getSubscriptionDisableReason()}
                            >
                                <Icon icon="subscription" />
                                <h4 className="mt-1 mb-0">Subscription</h4>
                                {!isProfessionalOrAbove && (
                                    <CounterBadge
                                        count={subscriptionsDatabaseCount}
                                        limit={subscriptionsDatabaseLimit}
                                        hideNotReached
                                    />
                                )}
                            </TaskItem>
                        </Row>
                        <HrHeader>ETL (RavenDB ⇛ TARGET)</HrHeader>
                        <Row className="gy-sm">
                            <TaskItem
                                title="Create new RavenDB ETL task"
                                href={appUrl.forEditRavenEtl(db.name)}
                                className="ravendb-etl"
                                target="RavenETL"
                            >
                                <Icon icon="ravendb-etl" />
                                <h4 className="mt-1 mb-0">RavenDB ETL</h4>
                                {!hasRavenDbEtl && <LicenseRestrictedBadge licenseRequired="Professional +" />}
                            </TaskItem>

                            <TaskItem
                                title="Create new Elasticsearch ETL task"
                                href={appUrl.forEditElasticSearchEtl(db.name)}
                                className="elastic-etl"
                                target="ElasticSearchETL"
                            >
                                <Icon icon="elastic-search-etl" />
                                <h4 className="mt-1 mb-0">Elasticsearch ETL</h4>
                                {!hasElasticSearchEtl && <LicenseRestrictedBadge licenseRequired="Enterprise" />}
                            </TaskItem>

                            <TaskItem
                                title="Create new Kafka ETL task"
                                href={appUrl.forEditKafkaEtl(db.name)}
                                className="kafka-etl"
                                target="KafkaETL"
                                disabled={isSharded}
                                disableReason={getDisableReasonForSharded()}
                            >
                                <Icon icon="kafka-etl" />
                                <h4 className="mt-1 mb-0">Kafka ETL</h4>
                                {!hasKafkaEtl && <LicenseRestrictedBadge licenseRequired="Enterprise" />}
                            </TaskItem>

                            <TaskItem
                                title="Create new SQL ETL task"
                                href={appUrl.forEditSqlEtl(db.name)}
                                className="sql-etl"
                                target="SqlETL"
                            >
                                <Icon icon="sql-etl" />
                                <h4 className="mt-1 mb-0">SQL ETL</h4>
                                {!hasSqlEtl && <LicenseRestrictedBadge licenseRequired="Professional +" />}
                            </TaskItem>

                            <TaskItem
                                title="Create new Snowflake ETL task"
                                href={appUrl.forEditSnowflakeEtl(db.name)}
                                className="snowflake-etl"
                                target="SnowflakeETL"
                            >
                                <Icon icon="snowflake-etl" />
                                <h4 className="mt-1 mb-0">Snowflake ETL</h4>
                                {!hasSnowflakeEtl && <LicenseRestrictedBadge licenseRequired="Enterprise" />}
                            </TaskItem>

                            <TaskItem
                                title="Create new OLAP ETL task"
                                href={appUrl.forEditOlapEtl(db.name)}
                                className="olap-etl"
                                target="OlapETL"
                            >
                                <Icon icon="olap-etl" />
                                <h4 className="mt-1 mb-0">OLAP ETL</h4>
                                {!hasOlapEtl && <LicenseRestrictedBadge licenseRequired="Enterprise" />}
                            </TaskItem>

                            <TaskItem
                                title="Create new RabbitMQ ETL task"
                                href={appUrl.forEditRabbitMqEtl(db.name)}
                                className="rabbitmq-etl"
                                target="RabbitMqETL"
                                disabled={isSharded}
                                disableReason={getDisableReasonForSharded()}
                            >
                                <Icon icon="rabbitmq-etl" />
                                <h4 className="mt-1 mb-0">RabbitMQ ETL</h4>
                                {!hasRabbitMqEtl && <LicenseRestrictedBadge licenseRequired="Enterprise" />}
                            </TaskItem>

                            <TaskItem
                                title="Create new Azure Queue Storage ETL task"
                                href={appUrl.forEditAzureQueueStorageEtl(db.name)}
                                className="azure-queue-storage-etl"
                                target="AzureQueueStorageETL"
                                disabled={isSharded}
                                disableReason={getDisableReasonForSharded()}
                            >
                                <Icon icon="azure-queue-storage-etl" />
                                <h4 className="mt-1 mb-0">Azure Queue Storage ETL</h4>
                                {!hasAzureQueueStorageEtl && <LicenseRestrictedBadge licenseRequired="Enterprise" />}
                            </TaskItem>
                            <TaskItem
                                title="Create new Amazon SQS ETL task"
                                href={appUrl.forEditAmazonSqsEtl(db.name)}
                                className="amazon-sqs-etl"
                                target="AmazonSqsETL"
                                disabled={isSharded}
                                disableReason={getDisableReasonForSharded()}
                            >
                                <Icon icon="amazon-sqs-etl" />
                                <h4 className="mt-1 mb-0">Amazon SQS ETL</h4>
                                {!hasAmazonSqsEtl && <LicenseRestrictedBadge licenseRequired="Enterprise" />}
                            </TaskItem>
                        </Row>
                        <HrHeader>SINK (SOURCE ⇛ RavenDB)</HrHeader>
                        <Row className="gy-sm">
                            <TaskItem
                                title="Create new Kafka Sink task"
                                href={appUrl.forEditKafkaSink(db.name)}
                                className="kafka-sink"
                                target="KafkaSink"
                                disabled={isSharded}
                                disableReason={getDisableReasonForSharded()}
                            >
                                <Icon icon="kafka-sink" />
                                <h4 className="mt-1 mb-0">Kafka Sink</h4>
                                {!hasKafkaSink && <LicenseRestrictedBadge licenseRequired="Enterprise" />}
                            </TaskItem>

                            <TaskItem
                                title="Create new RabbitMQ Sink task"
                                href={appUrl.forEditRabbitMqSink(db.name)}
                                className="rabbitmq-sink"
                                target="RabbitMqSink"
                                disabled={isSharded}
                                disableReason={getDisableReasonForSharded()}
                            >
                                <Icon icon="rabbitmq-sink" />
                                <h4 className="mt-1 mb-0">RabbitMQ Sink</h4>
                                {!hasRabbitMqSink && <LicenseRestrictedBadge licenseRequired="Enterprise" />}
                            </TaskItem>
                        </Row>
                    </>
                )}
            </Modal.Body>
        </Modal>
    );
}

interface TaskItemProps {
    title: string;
    href: string;
    className: string;
    target: string;
    children: ReactNode | ReactNode[];
    disabled?: boolean;
    disableReason?: string | ReactNode | ReactNode[];
}

function TaskItem(props: TaskItemProps) {
    const { title, href, className, target, children, disabled, disableReason } = props;

    const { reportEvent } = useEventsCollector();

    return (
        <Col xs="6" md="4" className="justify-content-center" title={title}>
            {disabled ? (
                <OverlayTrigger overlay={<Tooltip id={target}>{disableReason}</Tooltip>}>
                    <div id={target} className={classNames("task-item", className, { "item-disabled": disabled })}>
                        {children}
                    </div>
                </OverlayTrigger>
            ) : (
                <a
                    href={href}
                    onClick={() => reportEvent(target, "new")}
                    id={target}
                    className={classNames("task-item no-decor cursor-pointer", className)}
                >
                    {children}
                </a>
            )}
        </Col>
    );
}
