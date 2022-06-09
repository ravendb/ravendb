import React from "react";
import useInterval from "hooks/useInterval";
import { useServices } from "hooks/useServices";
import database from "models/resources/database";
import EtlTaskProgress = Raven.Server.Documents.ETL.Stats.EtlTaskProgress;
import useTimeout from "hooks/useTimeout";

interface OngoingTaskProgressProviderProps {
    db: database;
    onEtlProgress: (progress: EtlTaskProgress[], location: databaseLocationSpecifier) => void;
}

export function OngoingTaskProgressProvider(props: OngoingTaskProgressProviderProps): JSX.Element {
    const { db, onEtlProgress } = props;
    const { tasksService } = useServices();

    const locations = db.getLocations();

    const loadProgress = () => {
        locations.forEach(async (location) => {
            const progressResponse = await tasksService.getProgress(db, location);
            onEtlProgress(progressResponse.Results, location);
        });
    };

    useInterval(loadProgress, 3_000);
    useTimeout(loadProgress, 500);

    return null;
}
