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
import RichAlert from "components/common/RichAlert";
import PopoverWithHoverWrapper from "components/common/PopoverWithHoverWrapper";
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

    return (
        <>
            <RichPanelDetails>
                <TaskPanelToggleButton detailsVisible={detailsVisible} toggleDetails={toggleDetails} />
                <RichPanelDetailItem label="Last activity time">{lastActivityTime}</RichPanelDetailItem>
                <TaskPanelErrors
                    errorCount={errorCount}
                    errorsByLocation={errorsByLocation}
                    goToTaskErrors={goToTaskErrors}
                />
            </RichPanelDetails>
            <Collapse in={detailsVisible}>
                <div>
                    {data.shared.error && (
                        <div className="mx-3 mt-3">
                            <RichAlert variant="danger">{data.shared.error}</RichAlert>
                        </div>
                    )}
                    {data.shared.healthIssue && (
                        <div className="mx-3 mt-3">
                            <RichAlert variant="warning">{data.shared.healthIssue}</RichAlert>
                        </div>
                    )}
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
                            <RichPanelDetailItem label="Time since last activity">
                                {secondsSinceLastActivity}
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
                    {data.shared.error && (
                        <PopoverWithHoverWrapper message={data.shared.error}>
                            <Icon icon="danger" color="danger" margin="mx-1" />
                        </PopoverWithHoverWrapper>
                    )}
                    {data.shared.healthIssue && (
                        <PopoverWithHoverWrapper message={data.shared.healthIssue}>
                            <Icon icon="warning" color="warning" margin="mx-1" />
                        </PopoverWithHoverWrapper>
                    )}
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
