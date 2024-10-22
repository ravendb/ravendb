import useInterval from "hooks/useInterval";
import { useServices } from "hooks/useServices";
import EtlTaskProgress = Raven.Server.Documents.ETL.Stats.EtlTaskProgress;
import ReplicationTaskProgress = Raven.Server.Documents.Replication.Stats.ReplicationTaskProgress;
import useTimeout from "hooks/useTimeout";
import DatabaseUtils from "components/utils/DatabaseUtils";
import { databaseSelectors } from "components/common/shell/databaseSliceSelectors";
import { useAppSelector } from "components/store";

interface OngoingTaskProgressProviderProps {
    onEtlProgress: (progress: EtlTaskProgress[], location: databaseLocationSpecifier) => void;
    onReplicationProgress: (progress: ReplicationTaskProgress[], location: databaseLocationSpecifier) => void;
}

export function OngoingTaskProgressProvider(props: OngoingTaskProgressProviderProps): JSX.Element {
    const { onEtlProgress, onReplicationProgress } = props;
    const { tasksService } = useServices();

    const db = useAppSelector(databaseSelectors.activeDatabase);
    const locations = DatabaseUtils.getLocations(db);

    const loadProgress = () => {
        locations.forEach(async (location) => {
            const etlProgressTask = tasksService.getEtlProgress(db.name, location);
            const replicationProgressTask = tasksService.getReplicationProgress(db.name, location);

            const etlProgressResponse = await etlProgressTask;
            onEtlProgress(etlProgressResponse.Results, location);

            const replicationProgressResponse = await replicationProgressTask;
            onReplicationProgress(replicationProgressResponse.Results, location);
        });
    };

    useInterval(loadProgress, 8_000);
    useTimeout(loadProgress, 500);

    return null;
}
