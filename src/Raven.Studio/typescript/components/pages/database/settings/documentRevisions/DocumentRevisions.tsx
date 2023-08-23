import React, { useEffect, useState } from "react";
import { Button, Col, Row } from "reactstrap";
import { AboutViewAnchored, AboutViewHeading, AccordionItemWrapper } from "components/common/AboutView";
import { Icon } from "components/common/Icon";
import { Checkbox } from "components/common/Checkbox";
import { HrHeader } from "components/common/HrHeader";
import { FlexGrow } from "components/common/FlexGrow";
import { EmptySet } from "components/common/EmptySet";
import EditRevision, {
    EditRevisionConfigType,
    EditRevisionTaskType,
} from "components/pages/database/settings/documentRevisions/EditRevision";
import EnforceConfiguration from "components/pages/database/settings/documentRevisions/EnforceConfiguration";
import { todo } from "common/developmentHelper";
import { NonShardedViewProps } from "components/models/common";
import { LoadingView } from "components/common/LoadingView";
import {
    DocumentRevisionsConfig,
    documentRevisionsActions,
    documentRevisionsSelectors,
} from "./store/documentRevisionsSlice";
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

todo("Feature", "ANY", "Connect SelectionActions component");
todo("Feature", "ANY", "Component for limit revisions by age inputs (dd/hh/mm/ss)");
todo("Feature", "Matteo", "Add the Revert revisions view");
todo("Other", "Danielle", "Text for About this view");
todo("Other", "ANY", "Test the view");

interface EditRevisionData {
    onConfirm: (config: DocumentRevisionsConfig) => void;
    configType: EditRevisionConfigType;
    taskType: EditRevisionTaskType;
    toggle: () => void;
    config?: DocumentRevisionsConfig;
}

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

    useDirtyFlag(isAnyModified);

    const dispatch = useAppDispatch();

    useEffect(() => {
        dispatch(documentRevisionsActions.fetchConfigs(db));
    }, [db, dispatch]);

    const { databasesService } = useServices();

    const asyncSaveConfigs = useAsyncCallback(async () => {
        const config: Raven.Client.Documents.Operations.Revisions.RevisionsConfiguration = {
            Default: defaultDocumentsConfig ? _.omit(defaultDocumentsConfig, "Name") : null,
            Collections: Object.fromEntries(collectionConfigs.map((x) => [x.Name, _.omit(x, "Name")])),
        };

        const conflictsConfig: Raven.Client.Documents.Operations.Revisions.RevisionsCollectionConfiguration = _.omit(
            defaultConflictsConfig,
            "Name"
        );

        const promises = [
            databasesService.saveRevisionsConfiguration(db, config),
            databasesService.saveRevisionsForConflictsConfiguration(db, conflictsConfig),
        ];

        await Promise.all(promises);
        messagePublisher.reportSuccess("Revisions configuration has been saved");
    });

    const asyncEnforceRevisionsConfiguration = useAsyncCallback(async () => {
        const dto = await databasesService.enforceRevisionsConfiguration(db);

        // TODO kalczur openDetailsForOperationById does not work for sharded db

        notificationCenter.instance.openDetailsForOperationById(db, dto.OperationId);
    });

    const onEditRevision = (editRevisionData: Omit<EditRevisionData, "toggle">) => {
        setEditRevisionData({
            ...editRevisionData,
            toggle: () => setEditRevisionData(null),
        });
    };

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
                        <AboutViewHeading title="Document Revisions" icon="revisions" />
                        <div className="d-flex gap-2">
                            <ButtonWithSpinner
                                color="primary"
                                icon="save"
                                disabled={!isAnyModified}
                                onClick={asyncSaveConfigs.execute}
                                isSpinning={asyncSaveConfigs.status === "loading"}
                            >
                                Save
                            </ButtonWithSpinner>
                            <FlexGrow />
                            <Button color="secondary">
                                <Icon icon="revert-revisions" />
                                Revert revisions
                            </Button>
                            <ButtonWithSpinner
                                color="secondary"
                                onClick={toggleEnforceConfigurationModal}
                                isSpinning={asyncEnforceRevisionsConfiguration.status === "loading"}
                            >
                                <Icon icon="rocket" />
                                Enforce configuration
                            </ButtonWithSpinner>
                        </div>
                        <div className="mt-5">
                            <Checkbox
                                selected={false}
                                indeterminate={null}
                                toggleSelection={() => null}
                                color="primary"
                                title="Select all or none"
                                size="lg"
                            >
                                <span className="small-label">Select All</span>
                            </Checkbox>
                        </div>
                        <div className="mt-5">
                            <HrHeader
                                right={
                                    !defaultDocumentsConfig ? (
                                        <Button
                                            color="info"
                                            size="sm"
                                            className="rounded-pill"
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
                            <DocumentRevisionsConfigPanel
                                config={defaultDocumentsConfig}
                                onToggle={() =>
                                    dispatch(
                                        documentRevisionsActions.toggleConfigState({
                                            name: defaultDocumentsConfig.Name,
                                        })
                                    )
                                }
                                onDelete={() =>
                                    dispatch(
                                        documentRevisionsActions.deleteConfig({
                                            name: defaultDocumentsConfig.Name,
                                        })
                                    )
                                }
                                onOnEdit={() =>
                                    onEditRevision({
                                        taskType: "edit",
                                        configType: "defaultDocument",
                                        onConfirm: (config) => dispatch(documentRevisionsActions.editConfig(config)),
                                        config: defaultDocumentsConfig,
                                    })
                                }
                            />
                            <DocumentRevisionsConfigPanel
                                config={defaultConflictsConfig}
                                onToggle={() =>
                                    dispatch(
                                        documentRevisionsActions.toggleConfigState({
                                            name: defaultConflictsConfig.Name,
                                        })
                                    )
                                }
                                onOnEdit={() =>
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
                                    !isAllCollectionsAdded ? (
                                        <Button
                                            color="info"
                                            size="sm"
                                            className="rounded-pill"
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
                                        config={config}
                                        onToggle={() =>
                                            dispatch(documentRevisionsActions.toggleConfigState({ name: config.Name }))
                                        }
                                        onDelete={() =>
                                            dispatch(documentRevisionsActions.deleteConfig({ name: config.Name }))
                                        }
                                        onOnEdit={() =>
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
