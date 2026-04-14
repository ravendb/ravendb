import {
    RichPanel,
    RichPanelHeader,
    RichPanelInfo,
    RichPanelName,
    RichPanelActions,
    RichPanelDetailItem,
    RichPanelDetails,
} from "components/common/RichPanel";
import Button from "react-bootstrap/Button";
import { Icon } from "components/common/Icon";
import { Connection, StudioConnectionType } from "./connectionStringsTypes";
import { accessManagerSelectors } from "components/common/shell/accessManagerSliceSelectors";
import { useAppSelector } from "components/store";
import { useAsyncCallback } from "react-async-hook";
import ButtonWithSpinner from "components/common/ButtonWithSpinner";
import { useServices } from "components/hooks/useServices";
import { connectionStringsActions } from "./store/connectionStringsSlice";
import { useDispatch } from "react-redux";
import useConfirm from "components/common/ConfirmDialog";
import useUniqueId from "components/hooks/useUniqueId";
import { databaseSelectors } from "components/common/shell/databaseSliceSelectors";
import { ConditionalPopover } from "components/common/ConditionalPopover";
import copyToClipboard from "common/copyToClipboard";
import classNames from "classnames";
import { useAppUrls } from "components/hooks/useAppUrls";

interface ConnectionStringsPanelProps {
    connection: Connection;
    isServerwide?: boolean;
    onConnectionDeleted?: (connection: Connection) => void;
    onEditConnection?: (connection: Connection) => void;
}

export default function ConnectionStringsPanel(props: ConnectionStringsPanelProps) {
    const { connection, isServerwide = false, onConnectionDeleted, onEditConnection } = props;

    const confirm = useConfirm();
    const dispatch = useDispatch();
    const { tasksService } = useServices();

    const deleteButtonId = useUniqueId("delete");
    const isDeleteDisabled = connection.usedByTasks?.length > 0;

    const databaseName = useAppSelector(databaseSelectors.activeDatabaseName);
    const hasDatabaseAdminAccess = useAppSelector(accessManagerSelectors.getHasDatabaseAdminAccess)();
    const hasClusterAdminAccess = useAppSelector(accessManagerSelectors.isClusterAdminOrClusterNode);
    const hasWriteAccess = isServerwide ? hasClusterAdminAccess : hasDatabaseAdminAccess;
    const { appUrl } = useAppUrls();
    const isFromServerWide = !isServerwide && connection.excludedDatabases !== undefined;

    const asyncDelete = useAsyncCallback(async () => {
        if (isServerwide) {
            await tasksService.deleteServerWideConnectionString(getDtoEtlType(connection.type), connection.name);
            onConnectionDeleted?.(connection);
        } else {
            await tasksService.deleteConnectionString(databaseName, getDtoEtlType(connection.type), connection.name);
            dispatch(connectionStringsActions.connectionDeleted(connection));
        }
    });

    const onDelete = async () => {
        const isConfirmed = await confirm({
            title: (
                <span>
                    Delete <strong>{connection.name}</strong> connection string?
                </span>
            ),
            icon: "trash",
            actionColor: "danger",
            confirmText: "Delete",
        });

        if (isConfirmed) {
            await asyncDelete.execute();
        }
    };

    return (
        <RichPanel className="flex-row">
            <div className="flex-grow-1">
                <RichPanelHeader>
                    <RichPanelInfo>
                        <RichPanelName>{connection.name}</RichPanelName>
                    </RichPanelInfo>
                    {hasWriteAccess && (
                        <ConditionalPopover
                            conditions={{
                                isActive: isFromServerWide,
                                message: (
                                    <>
                                        This connection string is managed server-wide. To edit or delete it, go to{" "}
                                        <a href={appUrl.forServerwideConnectionStrings()}>
                                            Server-Wide Connection Strings
                                        </a>
                                        .
                                    </>
                                ),
                            }}
                        >
                            <div className={classNames({ "item-disabled pe-none": isFromServerWide })}>
                                <RichPanelActions>
                                    <Button
                                        variant="secondary"
                                        title="Edit connection string"
                                        onClick={() =>
                                            isServerwide
                                                ? onEditConnection?.(connection)
                                                : dispatch(
                                                      connectionStringsActions.editConnectionModalOpened(connection)
                                                  )
                                        }
                                    >
                                        <Icon icon="edit" margin="m-0" />
                                    </Button>
                                    <ConditionalPopover
                                        conditions={{
                                            isActive: isDeleteDisabled,
                                            message: "Connection string is being used by an ongoing task",
                                        }}
                                    >
                                        <div id={deleteButtonId}>
                                            <ButtonWithSpinner
                                                variant="danger"
                                                title="Delete connection string"
                                                disabled={isDeleteDisabled}
                                                onClick={onDelete}
                                                icon="trash"
                                                isSpinning={asyncDelete.loading}
                                            />
                                        </div>
                                    </ConditionalPopover>
                                </RichPanelActions>
                            </div>
                        </ConditionalPopover>
                    )}
                </RichPanelHeader>

                {"identifier" in connection && (
                    <RichPanelDetails className="p-0">
                        <RichPanelDetailItem label="Identifier">
                            {connection.identifier}
                            <Button
                                variant="link"
                                onClick={() =>
                                    copyToClipboard.copy(connection.identifier, "Identifier copied to clipboard")
                                }
                                size="xs"
                            >
                                <Icon icon="copy-to-clipboard" />
                            </Button>
                        </RichPanelDetailItem>
                    </RichPanelDetails>
                )}
            </div>
        </RichPanel>
    );
}

function getDtoEtlType(
    type: StudioConnectionType
): Raven.Client.Documents.Operations.ConnectionStrings.ConnectionStringType {
    switch (type) {
        case "Kafka":
        case "RabbitMQ":
        case "AzureQueueStorage":
        case "AmazonSqs":
        case "AzureServiceBus":
            return "Queue";
        default:
            return type;
    }
}
