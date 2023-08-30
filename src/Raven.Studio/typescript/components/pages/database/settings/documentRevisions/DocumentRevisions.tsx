import React, { useEffect, useState } from "react";
import { Button, Col, Row, UncontrolledTooltip } from "reactstrap";
import { AboutViewAnchored, AboutViewHeading, AccordionItemWrapper } from "components/common/AboutView";
import { Icon } from "components/common/Icon";
import { HrHeader } from "components/common/HrHeader";
import { FlexGrow } from "components/common/FlexGrow";
import { EmptySet } from "components/common/EmptySet";
import EditRevision, {
    EditRevisionConfigType,
    EditRevisionTaskType,
} from "components/pages/database/settings/documentRevisions/EditRevision";
import EnforceConfiguration from "components/pages/database/settings/documentRevisions/EnforceConfiguration";
import { todo, shardingTodo } from "common/developmentHelper";
import { NonShardedViewProps } from "components/models/common";
import { LoadingView } from "components/common/LoadingView";
import { DocumentRevisionsConfig, documentRevisionsActions } from "./store/documentRevisionsSlice";
import { documentRevisionsSelectors } from "./store/documentRevisionsSliceSelectors";
import { useAppDispatch, useAppSelector } from "components/store";
import { LoadError } from "components/common/LoadError";
import DocumentRevisionsConfigPanel from "./DocumentRevisionsConfigPanel";
import useBoolean from "components/hooks/useBoolean";
import ButtonWithSpinner from "components/common/ButtonWithSpinner";
import { useAsyncCallback } from "react-async-hook";
import { useServices } from "components/hooks/useServices";
import messagePublisher from "common/messagePublisher";
import notificationCenter from "common/notifications/notificationCenter";
import { useDirtyFlag } from "components/hooks/useDirtyFlag";
import { collectionsTrackerSelectors } from "components/common/shell/collectionsTrackerSlice";
import DocumentRevisionsSelectActions from "./DocumentRevisionsSelectActions";
import { StickyHeader } from "components/common/StickyHeader";
import { useEventsCollector } from "components/hooks/useEventsCollector";
import { useAppUrls } from "components/hooks/useAppUrls";
import { accessManagerSelectors } from "components/common/shell/accessManagerSlice";

interface EditRevisionData {
    onConfirm: (config: DocumentRevisionsConfig) => void;
    configType: EditRevisionConfigType;
    taskType: EditRevisionTaskType;
    toggle: () => void;
    config?: DocumentRevisionsConfig;
}

todo("Feature", "Damian", "Add the Revert revisions view");

export default function DocumentRevisions({ db }: NonShardedViewProps) {
    const { value: isEnforceConfigurationModalOpen, toggle: toggleEnforceConfigurationModal } = useBoolean(false);
    const [editRevisionData, setEditRevisionData] = useState<EditRevisionData>(null);

    const loadStatus = useAppSelector(documentRevisionsSelectors.loadStatus);
    const defaultDocumentsConfig = useAppSelector(documentRevisionsSelectors.defaultDocumentsConfig);
    const defaultConflictsConfig = useAppSelector(documentRevisionsSelectors.defaultConflictsConfig);
    const collectionConfigs = useAppSelector(documentRevisionsSelectors.collectionConfigs);
    const isAnyModified = useAppSelector(documentRevisionsSelectors.isAnyModified);

    const allCollectionNames = useAppSelector(collectionsTrackerSelectors.collectionNames);
    const isAllCollectionsAdded = allCollectionNames.length === collectionConfigs.length;

    const isDatabaseAdmin =
        useAppSelector(accessManagerSelectors.effectiveDatabaseAccessLevel(db.name)) === "DatabaseAdmin";

    useDirtyFlag(isAnyModified);
    const dispatch = useAppDispatch();
    const { forCurrentDatabase: urls } = useAppUrls();

    useEffect(() => {
        dispatch(documentRevisionsActions.fetchConfigs(db));
    }, [db, dispatch]);

    const { databasesService } = useServices();
    const { reportEvent } = useEventsCollector();

    const asyncSaveConfigs = useAsyncCallback(async () => {
        reportEvent("revisions", "save");

        const promises = [
            databasesService.saveRevisionsConfiguration(db, {
                Default: mapToDto(defaultDocumentsConfig),
                Collections: Object.fromEntries(collectionConfigs.map((x) => [x.Name, mapToDto(x)])),
            }),
            databasesService.saveRevisionsForConflictsConfiguration(db, mapToDto(defaultConflictsConfig)),
        ];

        await Promise.all(promises);
        messagePublisher.reportSuccess("Revisions configuration has been saved");

        dispatch(documentRevisionsActions.saveConfigs());
    });

    const asyncEnforceRevisionsConfiguration = useAsyncCallback(async () => {
        const dto = await databasesService.enforceRevisionsConfiguration(db);

        shardingTodo("ANY", "openDetailsForOperationById() does not work for sharded db");
        notificationCenter.instance.openDetailsForOperationById(db, dto.OperationId);
    });

    const onEditRevision = (editRevisionData: Omit<EditRevisionData, "toggle">) => {
        if (editRevisionData.taskType === "new") {
            reportEvent("revisions", "create");
        }

        setEditRevisionData({
            ...editRevisionData,
            toggle: () => setEditRevisionData(null),
        });
    };

    const isSaveDisabled = !isAnyModified || asyncSaveConfigs.status === "loading";

    if (loadStatus === "idle" || loadStatus === "loading") {
        return <LoadingView />;
    }

    if (loadStatus === "failure") {
        return (
            <LoadError
                error="Unable to load document revisions"
                refresh={() => documentRevisionsActions.fetchConfigs(db)}
            />
        );
    }

    return (
        <div className="content-margin">
            {isEnforceConfigurationModalOpen && (
                <EnforceConfiguration
                    toggle={toggleEnforceConfigurationModal}
                    onConfirm={asyncEnforceRevisionsConfiguration.execute}
                />
            )}
            {editRevisionData && <EditRevision {...editRevisionData} />}

            <Col xxl={12}>
                <Row className="gy-sm">
                    <Col>
                        <AboutViewHeading title="Document Revisions" icon="revisions" marginBottom={2} />

                        {isDatabaseAdmin && (
                            <StickyHeader>
                                <Row>
                                    <div className="d-flex gap-2">
                                        <ButtonWithSpinner
                                            color="primary"
                                            icon="save"
                                            disabled={isSaveDisabled}
                                            onClick={asyncSaveConfigs.execute}
                                            isSpinning={asyncSaveConfigs.status === "loading"}
                                        >
                                            Save
                                        </ButtonWithSpinner>
                                        <FlexGrow />

                                        <a
                                            className="btn btn-secondary"
                                            href={urls.revertRevisions()}
                                            title="Revert all documents in the database to a specific point in time"
                                        >
                                            <Icon icon="revert-revisions" />
                                            Revert revisions
                                        </a>

                                        <UncontrolledTooltip target="enforceConfiguration">
                                            {isSaveDisabled
                                                ? "Enforce the defined revisions configuration on all documents per collection"
                                                : "Save current configuration before enforcing"}
                                        </UncontrolledTooltip>
                                        <div id="enforceConfiguration">
                                            <ButtonWithSpinner
                                                color="secondary"
                                                onClick={toggleEnforceConfigurationModal}
                                                disabled={isAnyModified}
                                                isSpinning={asyncEnforceRevisionsConfiguration.status === "loading"}
                                            >
                                                <Icon icon="rocket" />
                                                Enforce configuration
                                            </ButtonWithSpinner>
                                        </div>
                                    </div>
                                    <div className="mt-5">
                                        <DocumentRevisionsSelectActions />
                                    </div>
                                </Row>
                            </StickyHeader>
                        )}

                        <div className="mt-5">
                            <HrHeader
                                right={
                                    isDatabaseAdmin && !defaultDocumentsConfig ? (
                                        <Button
                                            color="info"
                                            size="sm"
                                            className="rounded-pill"
                                            title="Create a default revision configuration for all (non-conflicting) documents"
                                            onClick={() =>
                                                onEditRevision({
                                                    taskType: "new",
                                                    configType: "defaultDocument",
                                                    onConfirm: (config) =>
                                                        dispatch(documentRevisionsActions.addConfig(config)),
                                                })
                                            }
                                        >
                                            Add new
                                        </Button>
                                    ) : null
                                }
                            >
                                <Icon icon="default" />
                                Defaults
                            </HrHeader>
                            {defaultDocumentsConfig && (
                                <DocumentRevisionsConfigPanel
                                    isDatabaseAdmin={isDatabaseAdmin}
                                    config={defaultDocumentsConfig}
                                    onToggle={() =>
                                        dispatch(
                                            documentRevisionsActions.toggleConfigState(defaultDocumentsConfig.Name)
                                        )
                                    }
                                    onDelete={() =>
                                        dispatch(documentRevisionsActions.deleteConfig(defaultDocumentsConfig.Name))
                                    }
                                    onEdit={() =>
                                        onEditRevision({
                                            taskType: "edit",
                                            configType: "defaultDocument",
                                            onConfirm: (config) =>
                                                dispatch(documentRevisionsActions.editConfig(config)),
                                            config: defaultDocumentsConfig,
                                        })
                                    }
                                />
                            )}
                            <DocumentRevisionsConfigPanel
                                isDatabaseAdmin={isDatabaseAdmin}
                                config={defaultConflictsConfig}
                                onToggle={() =>
                                    dispatch(documentRevisionsActions.toggleConfigState(defaultConflictsConfig.Name))
                                }
                                onEdit={() =>
                                    onEditRevision({
                                        taskType: "edit",
                                        configType: "defaultConflicts",
                                        onConfirm: (config) => dispatch(documentRevisionsActions.editConfig(config)),
                                        config: defaultConflictsConfig,
                                    })
                                }
                            />
                        </div>
                        <div className="mt-5">
                            <HrHeader
                                right={
                                    isDatabaseAdmin && !isAllCollectionsAdded ? (
                                        <Button
                                            color="info"
                                            size="sm"
                                            className="rounded-pill"
                                            title="Create a revision configuration for a specific collection"
                                            onClick={() =>
                                                onEditRevision({
                                                    taskType: "new",
                                                    configType: "collectionSpecific",
                                                    onConfirm: (config) =>
                                                        dispatch(documentRevisionsActions.addConfig(config)),
                                                })
                                            }
                                        >
                                            Add new
                                        </Button>
                                    ) : null
                                }
                            >
                                <Icon icon="documents" />
                                Collections
                            </HrHeader>
                            {collectionConfigs.length > 0 ? (
                                collectionConfigs.map((config) => (
                                    <DocumentRevisionsConfigPanel
                                        key={config.Name}
                                        isDatabaseAdmin={isDatabaseAdmin}
                                        config={config}
                                        onToggle={() =>
                                            dispatch(documentRevisionsActions.toggleConfigState(config.Name))
                                        }
                                        onDelete={() => dispatch(documentRevisionsActions.deleteConfig(config.Name))}
                                        onEdit={() =>
                                            onEditRevision({
                                                taskType: "edit",
                                                configType: "collectionSpecific",
                                                onConfirm: (config) =>
                                                    dispatch(documentRevisionsActions.editConfig(config)),
                                                config,
                                            })
                                        }
                                    />
                                ))
                            ) : (
                                <EmptySet>No collection specific configuration has been defined</EmptySet>
                            )}
                        </div>
                    </Col>
                    <Col sm={12} lg={4}>
                        <AboutViewAnchored>
                            <AccordionItemWrapper
                                targetId="1"
                                icon="about"
                                color="info"
                                description="Get additional info on what this feature can offer you"
                                heading="About this view"
                            >
                                <p>
                                    <strong>Document Revisions</strong> is a feature that allows developers to keep
                                    track of changes made to a document over time. When a document is updated in
                                    RavenDB, a new revision is automatically created, preserving the previous version of
                                    the document. This is particularly useful for scenarios where historical data and
                                    versioning are crucial.
                                </p>
                                <hr />
                                <div className="small-label mb-2">useful links</div>
                                <a href="https://ravendb.net/l/SOMRWC/6.0/Csharp" target="_blank">
                                    <Icon icon="newtab" /> Docs - Document Revisions
                                </a>
                            </AccordionItemWrapper>
                        </AboutViewAnchored>
                    </Col>
                </Row>
            </Col>
        </div>
    );
}

function mapToDto(
    config: DocumentRevisionsConfig
): Raven.Client.Documents.Operations.Revisions.RevisionsCollectionConfiguration {
    return config ? _.omit(config, "Name") : null;
}
