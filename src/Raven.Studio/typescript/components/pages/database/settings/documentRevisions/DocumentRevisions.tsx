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
import { todo } from "common/developmentHelper";
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
import DocumentRevisionsSelectActions from "./DocumentRevisionsSelectActions";
import { StickyHeader } from "components/common/StickyHeader";
import { useEventsCollector } from "components/hooks/useEventsCollector";
import { useAppUrls } from "components/hooks/useAppUrls";
import { accessManagerSelectors } from "components/common/shell/accessManagerSliceSelectors";
import { licenseSelectors } from "components/common/shell/licenseSlice";
import { useRavenLink } from "components/hooks/useRavenLink";
import FeatureAvailabilitySummaryWrapper, {
    FeatureAvailabilityData,
} from "components/common/FeatureAvailabilitySummary";
import { useLimitedFeatureAvailability } from "components/utils/licenseLimitsUtils";
import { databaseSelectors } from "components/common/shell/databaseSliceSelectors";
import activeDatabaseTracker from "common/shell/activeDatabaseTracker";

interface EditRevisionData {
    onConfirm: (config: DocumentRevisionsConfig) => void;
    configType: EditRevisionConfigType;
    taskType: EditRevisionTaskType;
    toggle: () => void;
    config?: DocumentRevisionsConfig;
}

todo("Feature", "Damian", "Add the Revert revisions view");

export default function DocumentRevisions() {
    const databaseName = useAppSelector(databaseSelectors.activeDatabaseName);
    const hasDatabaseAdminAccess = useAppSelector(accessManagerSelectors.getHasDatabaseAdminAccess)();

    const { value: isEnforceConfigurationModalOpen, toggle: toggleEnforceConfigurationModal } = useBoolean(false);
    const [editRevisionData, setEditRevisionData] = useState<EditRevisionData>(null);

    const loadStatus = useAppSelector(documentRevisionsSelectors.loadStatus);
    const defaultDocumentsConfig = useAppSelector(documentRevisionsSelectors.defaultDocumentsConfig);
    const defaultConflictsConfig = useAppSelector(documentRevisionsSelectors.defaultConflictsConfig);
    const collectionConfigs = useAppSelector(documentRevisionsSelectors.collectionConfigs);
    const isAnyModified = useAppSelector(documentRevisionsSelectors.isAnyModified);

    const canSetupDefaultRevisionsConfiguration = useAppSelector(
        licenseSelectors.statusValue("CanSetupDefaultRevisionsConfiguration")
    );
    const maxNumberOfRevisionsToKeep = useAppSelector(licenseSelectors.statusValue("MaxNumberOfRevisionsToKeep"));
    const maxNumberOfRevisionAgeToKeepInDays = useAppSelector(
        licenseSelectors.statusValue("MaxNumberOfRevisionAgeToKeepInDays")
    );

    const featureAvailability = useLimitedFeatureAvailability({
        defaultFeatureAvailability,
        overwrites: [
            {
                featureName: defaultFeatureAvailability[0].featureName,
                value: canSetupDefaultRevisionsConfiguration,
            },
            {
                featureName: defaultFeatureAvailability[1].featureName,
                value: maxNumberOfRevisionsToKeep,
            },
            {
                featureName: defaultFeatureAvailability[2].featureName,
                value: maxNumberOfRevisionAgeToKeepInDays,
            },
        ],
    });

    useDirtyFlag(isAnyModified);
    const dispatch = useAppDispatch();
    const { forCurrentDatabase: urls } = useAppUrls();

    const documentRevisionsDocsLink = useRavenLink({ hash: "OFVLX8" });

    useEffect(() => {
        dispatch(documentRevisionsActions.fetchConfigs(databaseName));

        return () => {
            dispatch(documentRevisionsActions.reset());
        };
        // Changing the database causes re-mount
        // eslint-disable-next-line react-hooks/exhaustive-deps
    }, []);

    const { databasesService } = useServices();
    const { reportEvent } = useEventsCollector();

    const asyncSaveConfigs = useAsyncCallback(async () => {
        reportEvent("revisions", "save");

        const promises = [
            databasesService.saveRevisionsConfiguration(databaseName, {
                Default: mapToDto(defaultDocumentsConfig),
                Collections: Object.fromEntries(collectionConfigs.map((x) => [x.Name, mapToDto(x)])),
            }),
            databasesService.saveRevisionsForConflictsConfiguration(databaseName, mapToDto(defaultConflictsConfig)),
        ];

        await Promise.all(promises);
        messagePublisher.reportSuccess("Revisions configuration has been saved");

        dispatch(documentRevisionsActions.saveConfigs());
    });

    const asyncEnforceRevisionsConfiguration = useAsyncCallback(
        async (includeForceCreated: boolean, collections: string[]) => {
            const dto = await databasesService.enforceRevisionsConfiguration(
                databaseName,
                includeForceCreated,
                collections
            );

            notificationCenter.instance.openDetailsForOperationById(
                activeDatabaseTracker.default.database(),
                dto.OperationId
            );
        }
    );

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
                refresh={() => documentRevisionsActions.fetchConfigs(databaseName)}
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

                        {hasDatabaseAdminAccess && (
                            <StickyHeader>
                                <Row>
                                    <div className="d-flex flex-wrap gap-2">
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
                                    <div className="mt-3">
                                        <DocumentRevisionsSelectActions />
                                    </div>
                                </Row>
                            </StickyHeader>
                        )}

                        <div className="mt-5">
                            <HrHeader
                                right={
                                    hasDatabaseAdminAccess && !defaultDocumentsConfig ? (
                                        <>
                                            <div id="add-default-config-button">
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
                                                    disabled={!canSetupDefaultRevisionsConfiguration}
                                                >
                                                    <Icon icon="plus" />
                                                    Add new
                                                </Button>
                                            </div>
                                            {!canSetupDefaultRevisionsConfiguration && (
                                                <UncontrolledTooltip target="add-default-config-button">
                                                    <div className="p-3">
                                                        Your license does not allow you to set up default policy.
                                                    </div>
                                                </UncontrolledTooltip>
                                            )}
                                        </>
                                    ) : null
                                }
                            >
                                <Icon icon="default" />
                                Defaults
                            </HrHeader>
                            {defaultDocumentsConfig && (
                                <DocumentRevisionsConfigPanel
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
                                    hasDatabaseAdminAccess ? (
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
                                            <Icon icon="plus" />
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
                                description="Get additional info on this feature"
                                heading="About this view"
                            >
                                <p>
                                    Creating <strong>Document Revisions</strong> allows keeping track of changes made to
                                    a document over time.
                                </p>
                                <div>
                                    A document revision will be created when:
                                    <ul>
                                        <li>Revisions are defined and enabled for the document&apos;s collection.</li>
                                        <li>The document has been modified.</li>
                                    </ul>
                                </div>
                                <div>
                                    Define the revisions configuration in this view:
                                    <ul>
                                        <li>
                                            Under section DEFAULTS:
                                            <br />
                                            Set default revisions configuration for all non-conflicting and conflicting
                                            documents.
                                        </li>
                                        <li>
                                            Under section COLLECTIONS:
                                            <br />
                                            Set revisions configuration for specific collections, overriding the
                                            defaults.
                                        </li>
                                    </ul>
                                </div>
                                <div>
                                    This view also provides these options:
                                    <ul>
                                        <li>Revert all documents to a specific point in time.</li>
                                        <li>
                                            Enforce the current configuration on all existing revisions in the database
                                            per collection.
                                        </li>
                                    </ul>
                                </div>
                                <hr />
                                <div className="small-label mb-2">useful links</div>
                                <a href={documentRevisionsDocsLink} target="_blank">
                                    <Icon icon="newtab" /> Docs - Document Revisions
                                </a>
                            </AccordionItemWrapper>
                            <FeatureAvailabilitySummaryWrapper
                                isUnlimited={
                                    canSetupDefaultRevisionsConfiguration &&
                                    !maxNumberOfRevisionsToKeep &&
                                    !maxNumberOfRevisionAgeToKeepInDays
                                }
                                data={featureAvailability}
                            />
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

const defaultFeatureAvailability: FeatureAvailabilityData[] = [
    {
        featureName: "Default Policy",
        featureIcon: "default",
        community: { value: false },
        professional: { value: true },
        enterprise: { value: true },
    },
    {
        featureName: "Max revisions to keep",
        featureIcon: "revisions",
        community: { value: 2 },
        professional: { value: Infinity },
        enterprise: { value: Infinity },
    },
    {
        featureName: "Max retention time (days)",
        featureIcon: "clock",
        community: { value: 45 },
        professional: { value: Infinity },
        enterprise: { value: Infinity },
    },
];
