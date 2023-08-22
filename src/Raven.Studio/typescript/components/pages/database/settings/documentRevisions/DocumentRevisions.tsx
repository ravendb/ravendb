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
import { useAppDispatch } from "components/store";
import { LoadError } from "components/common/LoadError";
import { useSelector } from "react-redux";
import DocumentRevisionsConfigPanel from "./DocumentRevisionsConfigPanel";
import useBoolean from "components/hooks/useBoolean";
import ButtonWithSpinner from "components/common/ButtonWithSpinner";
import { useAsyncCallback } from "react-async-hook";
import { useServices } from "components/hooks/useServices";
import messagePublisher from "common/messagePublisher";
import notificationCenter from "common/notifications/notificationCenter";

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

    const { databasesService } = useServices();
    const dispatch = useAppDispatch();
    const state = useSelector(documentRevisionsSelectors.state);

    useEffect(() => {
        dispatch(documentRevisionsActions.fetchConfigs(db));
    }, [db, dispatch]);

    const asyncSaveConfigs = useAsyncCallback(async () => {
        const config: Raven.Client.Documents.Operations.Revisions.RevisionsConfiguration = {
            Default: _.omit(state.Config.Default, "Name"),
            Collections: Object.fromEntries(state.Config.Collections.map((x) => [x.Name, _.omit(x, "Name")])),
        };

        const conflictsConfig: Raven.Client.Documents.Operations.Revisions.RevisionsCollectionConfiguration = _.omit(
            state.ConflictsConfig,
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

        // console.log("kalczur operationIdDto", dto);
        // console.log("kalczur operationId", dto.OperationId);
        // TODO kalczur openDetailsForOperationById does not work for sharded db

        notificationCenter.instance.openDetailsForOperationById(db, dto.OperationId);
    });

    const onEditRevision = (editRevisionData: Omit<EditRevisionData, "toggle">) => {
        setEditRevisionData({
            ...editRevisionData,
            toggle: () => setEditRevisionData(null),
        });
    };

    if (state.FetchStatus === "loading") {
        return <LoadingView />;
    }

    if (state.FetchStatus === "error") {
        return (
            <LoadError
                error="Unable to load document revisions"
                refresh={() => documentRevisionsActions.fetchConfigs(db)}
            />
        );
    }

    return (
        <div className="content-margin">
            {editRevisionData && <EditRevision {...editRevisionData} />}
            <EnforceConfiguration
                isOpen={isEnforceConfigurationModalOpen}
                toggle={toggleEnforceConfigurationModal}
                onConfirm={asyncEnforceRevisionsConfiguration.execute}
            />
            <Col xxl={12}>
                <Row className="gy-sm">
                    <Col>
                        <AboutViewHeading title="Document Revisions" icon="revisions" />
                        <div className="d-flex gap-2">
                            <ButtonWithSpinner
                                color="primary"
                                icon="save"
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
                                    !state.Config.Default ? (
                                        <Button
                                            color="info"
                                            size="sm"
                                            className="rounded-pill"
                                            onClick={() =>
                                                onEditRevision({
                                                    taskType: "new",
                                                    configType: "defaultDocument",
                                                    onConfirm: (config) =>
                                                        dispatch(
                                                            documentRevisionsActions.addDocumentDefaultsConfig(config)
                                                        ),
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
                                config={state.Config.Default}
                                onToggle={() => dispatch(documentRevisionsActions.toggleDocumentDefaultsConfig())}
                                onDelete={() => dispatch(documentRevisionsActions.deleteDocumentDefaultsConfig())}
                                onOnEdit={() =>
                                    onEditRevision({
                                        taskType: "edit",
                                        configType: "defaultDocument",
                                        onConfirm: (config) =>
                                            dispatch(documentRevisionsActions.editDocumentDefaultsConfig(config)),
                                        config: state.Config.Default,
                                    })
                                }
                            />
                            <DocumentRevisionsConfigPanel
                                config={state.ConflictsConfig}
                                onToggle={() => dispatch(documentRevisionsActions.toggleConflictsConfig())}
                                onOnEdit={() =>
                                    onEditRevision({
                                        taskType: "edit",
                                        configType: "defaultConflicts",
                                        onConfirm: (config) =>
                                            dispatch(documentRevisionsActions.editConflictsConfig(config)),
                                        config: state.ConflictsConfig,
                                    })
                                }
                            />
                        </div>
                        <div className="mt-5">
                            <HrHeader
                                right={
                                    <Button
                                        color="info"
                                        size="sm"
                                        className="rounded-pill"
                                        onClick={() =>
                                            onEditRevision({
                                                taskType: "new",
                                                configType: "collectionSpecific",
                                                onConfirm: (config) =>
                                                    dispatch(documentRevisionsActions.addCollectionConfig(config)),
                                            })
                                        }
                                    >
                                        Add new
                                    </Button>
                                }
                            >
                                <Icon icon="documents" />
                                Collections
                            </HrHeader>
                            {state.Config.Collections.length > 0 ? (
                                state.Config.Collections.map((config) => (
                                    <DocumentRevisionsConfigPanel
                                        key={config.Name}
                                        config={config}
                                        onToggle={() =>
                                            dispatch(
                                                documentRevisionsActions.toggleCollectionConfig({ Name: config.Name })
                                            )
                                        }
                                        onDelete={() =>
                                            dispatch(
                                                documentRevisionsActions.deleteCollectionConfig({ Name: config.Name })
                                            )
                                        }
                                        onOnEdit={() =>
                                            onEditRevision({
                                                taskType: "edit",
                                                configType: "collectionSpecific",
                                                onConfirm: (config) =>
                                                    dispatch(documentRevisionsActions.editCollectionConfig(config)),
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
