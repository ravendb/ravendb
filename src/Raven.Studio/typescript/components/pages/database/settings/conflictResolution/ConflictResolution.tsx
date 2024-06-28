import React, { useEffect } from "react";
import { Button, Card, CardBody, Col, Row, UncontrolledTooltip } from "reactstrap";
import { AboutViewHeading } from "components/common/AboutView";
import { Icon } from "components/common/Icon";
import { useAppDispatch, useAppSelector } from "components/store";
import { accessManagerSelectors } from "components/common/shell/accessManagerSlice";
import { HrHeader } from "components/common/HrHeader";
import ConflictResolutionConfigPanel from "components/pages/database/settings/conflictResolution/ConflictResolutionConfigPanel";
import { Switch } from "components/common/Checkbox";
import ButtonWithSpinner from "components/common/ButtonWithSpinner";
import { LoadError } from "components/common/LoadError";
import { LazyLoad } from "components/common/LazyLoad";
import {
    conflictResolutionSelectors,
    conflictResolutionActions,
    ConflictResolutionCollectionConfig,
} from "./store/conflictResolutionSlice";
import { EmptySet } from "components/common/EmptySet";
import { useAsyncCallback } from "react-async-hook";
import { useServices } from "components/hooks/useServices";
import { useEventsCollector } from "components/hooks/useEventsCollector";
import ConflictResolutionAboutView from "./ConflictResolutionAboutView";
import { databaseSelectors } from "components/common/shell/databaseSliceSelectors";

export default function ConflictResolution() {
    const dispatch = useAppDispatch();
    const databaseName = useAppSelector(databaseSelectors.activeDatabaseName);
    const loadStatus = useAppSelector(conflictResolutionSelectors.loadStatus);
    const isResolveToLatest = useAppSelector(conflictResolutionSelectors.isResolveToLatest);
    const collectionConfigs = useAppSelector(conflictResolutionSelectors.collectionConfigs);
    const isDirty = useAppSelector(conflictResolutionSelectors.isDirty);
    const isSomeInEditMode = useAppSelector(conflictResolutionSelectors.isSomeInEditMode);

    const hasDatabaseAdminAccess = useAppSelector(accessManagerSelectors.hasDatabaseAdminAccess());

    const { databasesService } = useServices();
    const { reportEvent } = useEventsCollector();

    useEffect(() => {
        dispatch(conflictResolutionActions.fetchConfig(databaseName));

        return () => {
            dispatch(conflictResolutionActions.reset());
        };
        // Changing the database causes re-mount
        // eslint-disable-next-line react-hooks/exhaustive-deps
    }, []);

    const asyncSave = useAsyncCallback(async () => {
        reportEvent("conflict-resolution", "save");

        const response = await databasesService.saveConflictSolverConfiguration(
            databaseName,
            mapToDto(isResolveToLatest, collectionConfigs)
        );
        dispatch(conflictResolutionActions.allSaved(response.ConflictSolverConfig));
    });

    if (loadStatus === "failure") {
        return (
            <LoadError
                error="Unable to load conflict resolution"
                refresh={() => dispatch(conflictResolutionActions.fetchConfig(databaseName))}
            />
        );
    }

    return (
        <Row className="content-margin gy-sm">
            <Col>
                <AboutViewHeading title="Conflict Resolution" icon="conflicts-resolution" />
                <LazyLoad active={loadStatus === "idle" || loadStatus === "loading"}>
                    {hasDatabaseAdminAccess && (
                        <>
                            <div id="saveConflictResolutionScript" className="d-flex w-fit-content gap-3 mb-3">
                                <ButtonWithSpinner
                                    color="primary"
                                    icon="save"
                                    isSpinning={asyncSave.loading}
                                    onClick={asyncSave.execute}
                                    disabled={!isDirty || isSomeInEditMode}
                                >
                                    Save
                                </ButtonWithSpinner>
                            </div>
                            {isSomeInEditMode && (
                                <UncontrolledTooltip target="saveConflictResolutionScript">
                                    Please finish editing all scripts before saving
                                </UncontrolledTooltip>
                            )}
                        </>
                    )}
                    <div className="mb-3">
                        <HrHeader
                            right={
                                hasDatabaseAdminAccess && (
                                    <div id="addNewScriptButton">
                                        <Button
                                            color="info"
                                            size="sm"
                                            className="rounded-pill"
                                            title="Add a new Conflicts Resolution script"
                                            onClick={() => dispatch(conflictResolutionActions.add())}
                                        >
                                            <Icon icon="plus" />
                                            Add new
                                        </Button>
                                    </div>
                                )
                            }
                            count={collectionConfigs.length}
                        >
                            <Icon icon="documents" />
                            Collection-specific scripts
                        </HrHeader>
                        {collectionConfigs.length > 0 ? (
                            collectionConfigs.map((config) => (
                                <ConflictResolutionConfigPanel key={config.id} initialConfig={config} />
                            ))
                        ) : (
                            <EmptySet>No scripts have been defined</EmptySet>
                        )}
                    </div>
                    <Card>
                        <CardBody>
                            <Switch
                                color="primary"
                                selected={isResolveToLatest}
                                toggleSelection={() => dispatch(conflictResolutionActions.toggleIsResolveToLatest())}
                                disabled={!hasDatabaseAdminAccess}
                            >
                                If no script was defined for a collection, resolve the conflict using the latest version
                            </Switch>
                        </CardBody>
                    </Card>
                </LazyLoad>
            </Col>
            <Col sm={12} lg={4}>
                <ConflictResolutionAboutView />
            </Col>
        </Row>
    );
}

function mapToDto(
    isResolveToLatest: boolean,
    collectionConfigs: ConflictResolutionCollectionConfig[]
): Raven.Client.ServerWide.ConflictSolver {
    return {
        ResolveToLatest: isResolveToLatest,
        ResolveByCollection: Object.fromEntries(
            collectionConfigs.map((config) => [
                config.name,
                {
                    Script: config.script,
                    LastModifiedTime: null,
                },
            ])
        ),
    };
}
