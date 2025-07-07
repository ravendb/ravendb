import { useCallback } from "react";
import {
    BaseOngoingTaskPanelProps,
    ConnectionStringItem,
    ICanShowTransformationScriptPreview,
    OngoingTaskActions,
    OngoingTaskName,
    OngoingTaskResponsibleNode,
    OngoingTaskStatus,
    useTasksOperations,
} from "../../shared/shared";
import { OngoingTaskGenAiInfo } from "components/models/tasks";
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
import Collapse from "react-bootstrap/Collapse";
import Form from "react-bootstrap/Form";
import Button from "react-bootstrap/Button";
import { databaseSelectors } from "components/common/shell/databaseSliceSelectors";
import { useAppSelector } from "components/store";
import { accessManagerSelectors } from "components/common/shell/accessManagerSliceSelectors";
import copyToClipboard from "common/copyToClipboard";
import { Icon } from "components/common/Icon";
import { OngoingEtlTaskDistribution } from "../partials/OngoingEtlTaskDistribution";
import PopoverWithHoverWrapper from "components/common/PopoverWithHoverWrapper";

type GenAiPanelProps = BaseOngoingTaskPanelProps<OngoingTaskGenAiInfo>;

function Details(props: GenAiPanelProps & { canEdit: boolean }) {
    const { data, canEdit } = props;
    const { appUrl } = useAppUrls();
    const databaseName = useAppSelector(databaseSelectors.activeDatabaseName);
    const connectionStringsUrl = appUrl.forConnectionStrings(databaseName, "Ai", data.shared.connectionStringName);

    const identifier = data.shared.identifier;
    const nextBatchStartingPoint = data.shared.nextBatchStartingPoint;

    return (
        <RichPanelDetails>
            {identifier && (
                <RichPanelDetails className="p-0">
                    <RichPanelDetailItem label="Identifier">
                        {identifier}
                        <Button
                            variant="link"
                            onClick={() => copyToClipboard.copy(identifier, "Identifier copied to clipboard")}
                            size="xs"
                        >
                            <Icon icon="copy-to-clipboard" />
                        </Button>
                    </RichPanelDetailItem>
                </RichPanelDetails>
            )}
            {nextBatchStartingPoint && (
                <RichPanelDetails className="p-0">
                    <RichPanelDetailItem label="Next Batch Starting Point">
                        <PopoverWithHoverWrapper
                            message={nextBatchStartingPoint
                                .split(",")
                                .map((item) => item.trim())
                                .join(", ")}
                        >
                            <Icon icon="info" color="info" margin="m-0" />
                        </PopoverWithHoverWrapper>
                    </RichPanelDetailItem>
                </RichPanelDetails>
            )}
            <ConnectionStringItem
                connectionStringDefined
                canEdit={canEdit}
                connectionStringName={data.shared.connectionStringName}
                connectionStringsUrl={connectionStringsUrl}
            />
        </RichPanelDetails>
    );
}

export function GenAiPanel(props: GenAiPanelProps & ICanShowTransformationScriptPreview) {
    const { data, showItemPreview, toggleSelection, isSelected, onTaskOperation, isDeleting, isTogglingState } = props;

    const hasDatabaseAdminAccess = useAppSelector(accessManagerSelectors.getHasDatabaseAdminAccess)();
    const { forCurrentDatabase } = useAppUrls();

    const canEdit = hasDatabaseAdminAccess && !data.shared.serverWide;
    const editUrl = forCurrentDatabase.editGenAi(data.shared.taskId)();

    const { detailsVisible, toggleDetails, onEdit } = useTasksOperations(editUrl, props);

    const showPreview = useCallback(
        (transformationName: string) => {
            showItemPreview(data, transformationName);
        },
        [data, showItemPreview]
    );

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
                        toggleDetails={toggleDetails}
                        isDeleting={isDeleting(data.shared.taskId)}
                    />
                </RichPanelActions>
            </RichPanelHeader>
            <Collapse in={detailsVisible}>
                <div>
                    <Details {...props} canEdit={canEdit} />
                    <OngoingEtlTaskDistribution task={data} showPreview={showPreview} />
                </div>
            </Collapse>
        </RichPanel>
    );
}
