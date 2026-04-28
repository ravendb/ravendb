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
import { connectionStringsActions, connectionStringSelectors } from "./store/connectionStringsSlice";
import { useDispatch } from "react-redux";
import useConfirm from "components/common/ConfirmDialog";
import useUniqueId from "components/hooks/useUniqueId";
import { databaseSelectors } from "components/common/shell/databaseSliceSelectors";
import { ConditionalPopover } from "components/common/ConditionalPopover";
import copyToClipboard from "common/copyToClipboard";
import classNames from "classnames";
import { useAppUrls } from "components/hooks/useAppUrls";
import { serverWideConnectionStringPrefix } from "./connectionStringsUtils";

interface ConnectionStringsPanelProps {
    connection: Connection;
}

export default function ConnectionStringsPanel({ connection }: ConnectionStringsPanelProps) {
    const viewContext = useAppSelector(connectionStringSelectors.viewContext);
    const isServerWide = viewContext === "serverWideConnectionStrings";

    const confirm = useConfirm();
    const dispatch = useDispatch();
    const { tasksService } = useServices();

    const isFromServerWide = !isServerWide && connection.name?.startsWith(serverWideConnectionStringPrefix);

    const deleteButtonId = useUniqueId("delete");
    const isDeleteDisabled = connection.usedByTasks?.length > 0 || isFromServerWide;
    const isEditDisabled = connection.usedByTasks?.length > 0 || isFromServerWide;

    const databaseName = useAppSelector(databaseSelectors.activeDatabaseName);
    const hasDatabaseAdminAccess = useAppSelector(accessManagerSelectors.getHasDatabaseAdminAccess)();
    const hasClusterAdminAccess = useAppSelector(accessManagerSelectors.isClusterAdminOrClusterNode);
    const hasWriteAccess = isServerWide ? hasClusterAdminAccess : hasDatabaseAdminAccess;
    const { appUrl } = useAppUrls();

    const asyncDelete = useAsyncCallback(async () => {
        if (isServerWide) {
            await tasksService.deleteServerWideConnectionString(getDtoEtlType(connection.type), connection.name);
        } else {
            await tasksService.deleteConnectionString(databaseName, getDtoEtlType(connection.type), connection.name);
        }
        dispatch(connectionStringsActions.connectionDeleted(connection));
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
                                    <ConditionalPopover
                                        conditions={[
                                            {
                                                isActive: isFromServerWide,
                                                message: (
                                                    <>
                                                        This connection string is managed server-wide. To edit or delete
                                                        it, go to{" "}
                                                        <a href={appUrl.forServerwideConnectionStrings()}>
                                                            Server-Wide Connection Strings
                                                        </a>
                                                        .
                                                    </>
                                                ),
                                            },
                                            {
                                                isActive: isEditDisabled,
                                                message: "Connection string is being used by an ongoing task",
                                            },
                                        ]}
                                    >
                                        <div>
                                            <Button
                                                variant="secondary"
                                                title="Edit connection string"
                                                onClick={() =>
                                                    isServerWide
                                                        ? dispatch(
                                                              connectionStringsActions.serverWideEditConnectionOpened(
                                                                  connection
                                                              )
                                                          )
                                                        : dispatch(
                                                              connectionStringsActions.editConnectionModalOpened(
                                                                  connection
                                                              )
                                                          )
                                                }
                                                disabled={isFromServerWide}
                                            >
                                                <Icon icon="edit" margin="m-0" />
                                            </Button>
                                        </div>
                                    </ConditionalPopover>
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
                                                disabled={isDeleteDisabled || isFromServerWide}
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
