import {
    BaseOngoingTaskPanelProps,
    ConnectionStringItem,
    OngoingTaskActions,
    OngoingTaskName,
    OngoingTaskResponsibleNode,
    OngoingTaskStatus,
    useTasksOperations,
} from "../../shared/shared";
import { OngoingTaskCdcSinkInfo } from "components/models/tasks";
import { useAppUrls } from "hooks/useAppUrls";
import {
    RichPanel,
    RichPanelActions,
    RichPanelDetailItem,
    RichPanelDetails,
    RichPanelHeader,
    RichPanelInfo,
    RichPanelSelect,
} from "components/common/RichPanel";
import Form from "react-bootstrap/Form";
import { databaseSelectors } from "components/common/shell/databaseSliceSelectors";
import { useAppSelector } from "components/store";
import { accessManagerSelectors } from "components/common/shell/accessManagerSliceSelectors";
import { Icon } from "components/common/Icon";
import moment from "moment";
import genUtils from "common/generalUtils";
import { ErrorDetailItem } from "components/pages/database/tasks/ongoingTasks/partials/ErrorModal";
import { WarningDetails } from "components/pages/database/tasks/ongoingTasks/partials/WarningDetails";
import {
    getTaskConnectionStatus,
    TaskConnectionStatusItem,
} from "components/pages/database/tasks/ongoingTasks/partials/TaskConnectionStatus";
import {
    DetailsToggleProps,
    TaskPanelErrors,
    TaskPanelToggleButton,
} from "components/pages/database/tasks/ongoingTasks/panels/EtlPanelComponents";
import Collapse from "react-bootstrap/Collapse";
import {
    getTaskErrorCount,
    getTaskErrorCountByLocation,
} from "components/pages/database/tasks/ongoingTasks/panels/etlPanelUtils";
import { TaskErrorsWithLocation } from "components/pages/database/tasks/tasksErrors/utils/tasksErrorsUtils";

type CdcSinkPanelProps = BaseOngoingTaskPanelProps<OngoingTaskCdcSinkInfo> & {
    taskErrors?: TaskErrorsWithLocation[];
};

function Details(props: CdcSinkPanelProps & DetailsToggleProps & { canEdit: boolean }) {
    const { data, canEdit, detailsVisible, toggleDetails } = props;
    const databaseName = useAppSelector(databaseSelectors.activeDatabaseName);
    const { appUrl } = useAppUrls();

    const lastBatchTime = formatDate(data.shared.lastBatchTime);
    const lastActivityTime = formatDate(data.shared.lastActivityTime);
    const secondsSinceLastBatch = formatSeconds(data.shared.secondsSinceLastBatch);
    const secondsSinceLastActivity = formatSeconds(data.shared.secondsSinceLastActivity);
    const configuration = data.shared.configuration;
    const tables = configuration?.Tables ?? [];
    const enabledTables = tables.filter((x) => !x.Disabled).length;

    const errorCount = getTaskErrorCount(props.taskErrors ?? [], data.shared.taskName);
    const errorsByLocation = getTaskErrorCountByLocation(
        props.taskErrors ?? [],
        data.shared.taskName,
        data.responsibleLocations
    );
    const goToTaskErrors = appUrl.forTasksErrors(databaseName, { taskName: data.shared.taskName });
    const isReconnecting = getTaskConnectionStatus(data) === "Reconnect";

    return (
        <>
            <RichPanelDetails>
                <TaskPanelToggleButton detailsVisible={detailsVisible} toggleDetails={toggleDetails} />
                <TaskConnectionStatusItem task={data} reconnectDetails={data.shared.healthIssue} />
                <RichPanelDetailItem label="Last activity time">{lastActivityTime}</RichPanelDetailItem>
                <RichPanelDetailItem label="Time since last activity">{secondsSinceLastActivity}</RichPanelDetailItem>
                <TaskPanelErrors
                    errorCount={errorCount}
                    errorsByLocation={errorsByLocation}
                    goToTaskErrors={goToTaskErrors}
                />
                {data.shared.error && <ErrorDetailItem error={data.shared.error} />}
                {data.shared.healthIssue && !isReconnecting && (
                    <RichPanelDetailItem label="Health Issue">
                        <WarningDetails details={data.shared.healthIssue} title="Show health issue">
                            <span className="detail-item-value-truncate">{data.shared.healthIssue}</span>
                        </WarningDetails>
                    </RichPanelDetailItem>
                )}
            </RichPanelDetails>
            <Collapse in={detailsVisible}>
                <div>
                    <RichPanelDetails className="d-block">
                        <div className="hstack flex-wrap">
                            <ConnectionStringItem
                                connectionStringDefined
                                canEdit={canEdit}
                                connectionStringName={data.shared.connectionStringName}
                                connectionStringType="Sql"
                                databaseName={databaseName}
                            />
                            <RichPanelDetailItem label="Factory name">{data.shared.factoryName}</RichPanelDetailItem>
                            <RichPanelDetailItem label="Skip initial load">
                                {configuration?.SkipInitialLoad ? "Yes" : "No"}
                            </RichPanelDetailItem>
                            <RichPanelDetailItem label="Last checkpoint" contentClassName="text-break">
                                {data.shared.lastCheckpoint || "N/A"}
                            </RichPanelDetailItem>
                            <RichPanelDetailItem label="Tables">
                                {enabledTables} / {tables.length} enabled
                            </RichPanelDetailItem>
                        </div>
                        <div className="hstack">
                            <RichPanelDetailItem label="Last batch time">{lastBatchTime}</RichPanelDetailItem>
                            <RichPanelDetailItem label="Time since last batch">
                                {secondsSinceLastBatch}
                            </RichPanelDetailItem>
                        </div>
                    </RichPanelDetails>
                </div>
            </Collapse>
        </>
    );
}

export function CdcSinkPanel(props: CdcSinkPanelProps) {
    const { data, toggleSelection, isSelected, onTaskOperation, isDeleting, isTogglingState } = props;

    const hasDatabaseAdminAccess = useAppSelector(accessManagerSelectors.getHasDatabaseAdminAccess)();
    const { forCurrentDatabase } = useAppUrls();

    const canEdit = hasDatabaseAdminAccess && !data.shared.serverWide;
    const editUrl = forCurrentDatabase.editCdcSink(data.shared.taskId)();

    const { detailsVisible, toggleDetails, onEdit } = useTasksOperations(editUrl, props);

    return (
        <RichPanel>
            <RichPanelHeader>
                <RichPanelInfo>
                    {canEdit && (
                        <RichPanelSelect>
                            <Form.Check
                                type="checkbox"
                                onChange={(e) => toggleSelection(e.currentTarget.checked, data.shared)}
                                checked={isSelected(data.shared.taskId)}
                            />
                        </RichPanelSelect>
                    )}
                    <OngoingTaskName task={data} canEdit={canEdit} editUrl={editUrl} />
                </RichPanelInfo>
                <RichPanelActions>
                    <span>
                        <Icon icon="cdc-sink" />
                        CDC Sink
                    </span>
                    <OngoingTaskResponsibleNode task={data} />
                    <OngoingTaskStatus
                        task={data}
                        canEdit={canEdit}
                        onTaskOperation={onTaskOperation}
                        isTogglingState={isTogglingState(data.shared.taskId)}
                    />
                    <OngoingTaskActions
                        task={data}
                        canEdit={canEdit}
                        onEdit={onEdit}
                        onTaskOperation={onTaskOperation}
                        isDeleting={isDeleting(data.shared.taskId)}
                    />
                </RichPanelActions>
            </RichPanelHeader>
            <Details {...props} canEdit={canEdit} detailsVisible={detailsVisible} toggleDetails={toggleDetails} />
        </RichPanel>
    );
}

function formatDate(date?: string): string {
    return date ? moment.utc(date).local().format(genUtils.dateFormat) : "N/A";
}

function formatSeconds(seconds?: number): string {
    return seconds != null ? genUtils.formatDuration(moment.duration(seconds, "seconds"), true, 2) : "N/A";
}
