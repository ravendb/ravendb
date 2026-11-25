import {
    RichPanel,
    RichPanelHeader,
    RichPanelInfo,
    RichPanelName,
    RichPanelActions,
    RichPanelDetails,
    RichPanelDetailItem,
} from "components/common/RichPanel";
import Button from "react-bootstrap/Button";
import { Icon } from "components/common/Icon";
import { DocumentRevisionsConfig, documentRevisionsActions } from "./store/documentRevisionsSlice";
import { documentRevisionsSelectors } from "./store/documentRevisionsSliceSelectors";
import { useAppDispatch, useAppSelector } from "components/store";
import { accessManagerSelectors } from "components/common/shell/accessManagerSliceSelectors";
import generalUtils from "common/generalUtils";
import { useEventsCollector } from "components/hooks/useEventsCollector";

interface ConversationsRevisionsConfigPanelProps {
    config: DocumentRevisionsConfig;
    onEdit: () => void;
}

export default function ConversationsRevisionsConfigPanel({ config, onEdit }: ConversationsRevisionsConfigPanelProps) {
    const hasDatabaseAdminAccess = useAppSelector(accessManagerSelectors.getHasDatabaseAdminAccess)();
    const dispatch = useAppDispatch();
    const { reportEvent } = useEventsCollector();

    const originalConfig = useAppSelector(documentRevisionsSelectors.originalConfig(config.Name));
    const isModified = !_.isEqual(originalConfig, config);

    const formattedMinimumRevisionAgeToKeep = config.MinimumRevisionAgeToKeep
        ? generalUtils.formatTimeSpan(generalUtils.timeSpanToSeconds(config.MinimumRevisionAgeToKeep) * 1000, true)
        : null;

    const isDetailsVisible =
        config.MinimumRevisionsToKeep != null ||
        config.MinimumRevisionAgeToKeep != null ||
        config.PurgeOnDelete ||
        config.MaximumRevisionsToDeleteUponDocumentUpdate != null;

    return (
        <RichPanel className="flex-row">
            <div className="flex-grow-1">
                <RichPanelHeader className={!isDetailsVisible ? "h-100" : undefined}>
                    <RichPanelInfo>
                        <RichPanelName>
                            {config.Name}
                            {isModified && <span className="text-warning ms-1">*</span>}
                        </RichPanelName>
                    </RichPanelInfo>
                    {hasDatabaseAdminAccess && (
                        <RichPanelActions>
                            <Button variant="secondary" onClick={onEdit} title="Edit this revision configuration">
                                <Icon icon="edit" margin="m-0" />
                            </Button>
                            <Button
                                variant="danger"
                                onClick={() => {
                                    reportEvent("revisions", "remove");
                                    dispatch(documentRevisionsActions.configDeleted(config.Name));
                                }}
                                title="Delete this revision configuration"
                            >
                                <Icon icon="trash" margin="m-0" />
                            </Button>
                        </RichPanelActions>
                    )}
                </RichPanelHeader>
                {isDetailsVisible && (
                    <RichPanelDetails>
                        {config.MinimumRevisionsToKeep != null && (
                            <RichPanelDetailItem
                                label={
                                    <>
                                        <Icon icon="documents" />
                                        Keep
                                    </>
                                }
                            >
                                {config.MinimumRevisionsToKeep} revisions
                            </RichPanelDetailItem>
                        )}
                        {formattedMinimumRevisionAgeToKeep && (
                            <RichPanelDetailItem
                                label={
                                    <>
                                        <Icon icon="clock" />
                                        Retention time
                                    </>
                                }
                            >
                                {formattedMinimumRevisionAgeToKeep}
                            </RichPanelDetailItem>
                        )}
                        {config.PurgeOnDelete && (
                            <RichPanelDetailItem
                                label={
                                    <>
                                        <Icon icon="trash" />
                                        Purge on delete
                                    </>
                                }
                            >
                                Yes
                            </RichPanelDetailItem>
                        )}
                        {config.MaximumRevisionsToDeleteUponDocumentUpdate != null && (
                            <RichPanelDetailItem
                                label={
                                    <>
                                        <Icon icon="trash" />
                                        Max to delete on update
                                    </>
                                }
                            >
                                {config.MaximumRevisionsToDeleteUponDocumentUpdate}
                            </RichPanelDetailItem>
                        )}
                    </RichPanelDetails>
                )}
            </div>
        </RichPanel>
    );
}
