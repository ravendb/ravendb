import React from "react";
import {
    RichPanel,
    RichPanelActions,
    RichPanelDetailItem,
    RichPanelDetails,
    RichPanelHeader,
    RichPanelInfo,
    RichPanelSelect,
} from "components/common/RichPanel";
import {
    ConnectionStringItem,
    ICanShowTransformationScriptPreview,
    OngoingTaskActions,
    OngoingTaskName,
    OngoingTaskResponsibleNode,
    OngoingTaskStatus,
} from "../../shared/shared";
import { useAppUrls } from "hooks/useAppUrls";
import { OngoingTaskGenAiInfo } from "components/models/tasks";
import { OngoingEtlTaskDistribution } from "../partials/OngoingEtlTaskDistribution";
import Collapse from "react-bootstrap/Collapse";
import Form from "react-bootstrap/Form";
import Button from "react-bootstrap/Button";
import { databaseSelectors } from "components/common/shell/databaseSliceSelectors";
import { useAppSelector } from "components/store";
import { Icon } from "components/common/Icon";
import { EtlPanelBaseProps, useEtlPanel } from "./etlPanelUtils";
import { EtlPanelErrors, EtlPanelHealthBadge, EtlPanelProgressItem, EtlPanelToggleButton } from "./EtlPanelComponents";
import copyToClipboard from "common/copyToClipboard";
import PopoverWithHoverWrapper from "components/common/PopoverWithHoverWrapper";

type GenAiPanelProps = EtlPanelBaseProps<OngoingTaskGenAiInfo>;

export function GenAiPanel(props: GenAiPanelProps & ICanShowTransformationScriptPreview) {
    const { data, toggleSelection, isSelected, onTaskOperation, isDeleting, isTogglingState, etlStats } = props;

    const { forCurrentDatabase, appUrl } = useAppUrls();
    const editUrl = forCurrentDatabase.editGenAi(data.shared.taskId)();

    const {
        canEdit,
        goToTaskErrors,
        detailsVisible,
        toggleDetails,
        onEdit,
        showPreview,
        taskHealth,
        errorCount,
        errorsByLocation,
        etlProgress,
    } = useEtlPanel(props, editUrl);

    const databaseName = useAppSelector(databaseSelectors.activeDatabaseName);
    const connectionStringsUrl = appUrl.forConnectionStrings(databaseName, "Ai", data.shared.connectionStringName);

    const identifier = data.shared.identifier;
    const nextBatchStartingPoint = data.shared.nextBatchStartingPoint;

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
                        isDetailsOpen={detailsVisible}
                        isEtl
                    />
                </RichPanelActions>
            </RichPanelHeader>
            <RichPanelDetails>
                <EtlPanelToggleButton detailsVisible={detailsVisible} toggleDetails={toggleDetails} />
                <RichPanelDetailItem label="Type">
                    <Icon icon="genai" />
                    GenAI
                </RichPanelDetailItem>
                {identifier && (
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
                )}
                {nextBatchStartingPoint && (
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
                )}
                <ConnectionStringItem
                    connectionStringDefined
                    canEdit={canEdit}
                    connectionStringName={data.shared.connectionStringName}
                    connectionStringsUrl={connectionStringsUrl}
                />
                <EtlPanelHealthBadge taskHealth={taskHealth} />
                <EtlPanelErrors
                    errorCount={errorCount}
                    errorsByLocation={errorsByLocation}
                    goToTaskErrors={goToTaskErrors}
                />
                <EtlPanelProgressItem etlProgress={etlProgress} />
            </RichPanelDetails>
            <Collapse in={detailsVisible}>
                <div>
                    <OngoingEtlTaskDistribution task={data} showPreview={showPreview} etlStats={etlStats} />
                </div>
            </Collapse>
        </RichPanel>
    );
}
