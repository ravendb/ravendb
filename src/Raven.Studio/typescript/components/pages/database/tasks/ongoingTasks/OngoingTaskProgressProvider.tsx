import useInterval from "hooks/useInterval";
import { useServices } from "hooks/useServices";
import EtlTaskProgress = Raven.Server.Documents.ETL.Stats.EtlTaskProgress;
import useTimeout from "hooks/useTimeout";
import DatabaseUtils from "components/utils/DatabaseUtils";
import { databaseSelectors } from "components/common/shell/databaseSliceSelectors";
import { useAppSelector } from "components/store";

interface OngoingTaskProgressProviderProps {
    onEtlProgress: (progress: EtlTaskProgress[], location: databaseLocationSpecifier) => void;
}

export function OngoingTaskProgressProvider(props: OngoingTaskProgressProviderProps): JSX.Element {
    const { onEtlProgress } = props;
    const { tasksService } = useServices();

    const db = useAppSelector(databaseSelectors.activeDatabase);
    const locations = DatabaseUtils.getLocations(db);

    const loadProgress = () => {
        locations.forEach(async (location) => {
            const progressResponse = await tasksService.getProgress(db.name, location);
            onEtlProgress(progressResponse.Results, location);
        });
    };

    useInterval(loadProgress, 8_000);
    useTimeout(loadProgress, 500);

    return null;
}
