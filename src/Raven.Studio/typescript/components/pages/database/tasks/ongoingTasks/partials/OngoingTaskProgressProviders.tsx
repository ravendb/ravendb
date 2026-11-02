import useInterval from "hooks/useInterval";
import { useServices } from "hooks/useServices";
import EtlTaskProgress = Raven.Server.Documents.ETL.Stats.EtlTaskProgress;
import ReplicationTaskProgress = Raven.Server.Documents.Replication.Stats.ReplicationTaskProgress;
import useTimeout from "hooks/useTimeout";
import DatabaseUtils from "components/utils/DatabaseUtils";
import { databaseSelectors } from "components/common/shell/databaseSliceSelectors";
import { useAppSelector } from "components/store";
import InternalReplicationTaskProgress = Raven.Server.Documents.Replication.Stats.InternalReplicationTaskProgress;
import React from "react";
import { getRequestErrorMessage } from "components/utils/common";

interface InternalReplicationProgressProviderProps {
    onProgress: (progress: InternalReplicationTaskProgress[], location: databaseLocationSpecifier) => void;
    onError: (error: string, location: databaseLocationSpecifier) => void;
}

export function InternalReplicationProgressProvider(
    props: InternalReplicationProgressProviderProps
): React.JSX.Element {
    const { onProgress, onError } = props;
    const { tasksService } = useServices();

    const db = useAppSelector(databaseSelectors.activeDatabase);
    const locations = DatabaseUtils.getLocations(db);

    const loadProgress = () => {
        locations.forEach(async (location) => {
            try {
                const internalReplicationProgressResponse = await tasksService.getInternalReplicationProgress(
                    db.name,
                    location
                );
                onProgress(internalReplicationProgressResponse.Results, location);
            } catch (error) {
                onError(getRequestErrorMessage(error), location);
            }
        });
    };

    useInterval(loadProgress, 8_000);
    useTimeout(loadProgress, 500);

    return null;
}

interface ReplicationProgressProviderProps {
    onProgress: (progress: ReplicationTaskProgress[], location: databaseLocationSpecifier) => void;
}

export function ReplicationProgressProvider(props: ReplicationProgressProviderProps): React.JSX.Element {
    const { onProgress } = props;
    const { tasksService } = useServices();

    const db = useAppSelector(databaseSelectors.activeDatabase);
    const locations = DatabaseUtils.getLocations(db);

    const loadProgress = () => {
        locations.forEach(async (location) => {
            const replicationProgressResponse = await tasksService.getReplicationProgress(db.name, location);
            onProgress(replicationProgressResponse.Results, location);
        });
    };

    useInterval(loadProgress, 8_000);
    useTimeout(loadProgress, 500);

    return null;
}

interface EtlProgressProviderProps {
    onProgress: (progress: EtlTaskProgress[], location: databaseLocationSpecifier) => void;
}

export function EtlProgressProvider(props: EtlProgressProviderProps): React.JSX.Element {
    const { onProgress } = props;
    const { tasksService } = useServices();

    const db = useAppSelector(databaseSelectors.activeDatabase);
    const locations = DatabaseUtils.getLocations(db);

    const loadProgress = () => {
        locations.forEach(async (location) => {
            const etlProgressResponse = await tasksService.getEtlProgress(db.name, location);
            onProgress(etlProgressResponse.Results, location);
        });
    };

    useInterval(loadProgress, 8_000);
    useTimeout(loadProgress, 500);

    return null;
}
