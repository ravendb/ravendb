import {
    RichPanel,
    RichPanelActions,
    RichPanelDetailItem,
    RichPanelDetails,
    RichPanelHeader,
    RichPanelInfo,
    RichPanelName,
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
import { databaseSelectors } from "components/common/shell/databaseSliceSelectors";
import { ConditionalPopover } from "components/common/ConditionalPopover";
import copyToClipboard from "common/copyToClipboard";
import { useAppUrls } from "components/hooks/useAppUrls";
import { serverWideConnectionStringPrefix, getServerWideShortName } from "./connectionStringsUtils";
import { getAccessRequiredMessage } from "components/utils/accessUtils";

interface ConnectionStringsPanelProps {
    connection: Connection;
}

export default function ConnectionStringsPanel({ connection }: ConnectionStringsPanelProps) {
    const { appUrl } = useAppUrls();
    const viewContext = useAppSelector(connectionStringSelectors.viewContext);
    const databaseName = useAppSelector(databaseSelectors.activeDatabaseName);
    const hasDatabaseAdminAccess = useAppSelector(accessManagerSelectors.getHasDatabaseAdminAccess)();
    const hasOperatorAccess = useAppSelector(accessManagerSelectors.isOperatorOrAbove);

    const isServerWide = viewContext === "serverWideConnectionStrings";

    const hasWriteAccess = isServerWide ? hasOperatorAccess : hasDatabaseAdminAccess;

    const confirm = useConfirm();
    const dispatch = useDispatch();
    const { tasksService } = useServices();

    const isInheritedFromServerWide = !isServerWide && connection.name?.startsWith(serverWideConnectionStringPrefix);

    const isDeleteDisabled = connection.usedBy?.length > 0 || isInheritedFromServerWide || !hasWriteAccess;
    const isEditDisabled = isInheritedFromServerWide || !hasWriteAccess;

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
                    <RichPanelActions>
                        <ConditionalPopover
                            conditions={[
                                {
                                    isActive: !hasWriteAccess,
                                    message: getAccessRequiredMessage(isServerWide ? "Operator" : "DatabaseAdmin"),
                                },
                                {
                                    isActive: isInheritedFromServerWide,
                                    message: (
                                        <>
                                            This connection string is managed server-wide. To edit or delete it, go to{" "}
                                            <a
                                                href={appUrl.forServerWideConnectionStrings(
                                                    connection.type,
                                                    getServerWideShortName(connection.name)
                                                )}
                                            >
                                                Server-Wide Connection Strings
                                            </a>
                                            .
                                        </>
                                    ),
                                },
                            ]}
                        >
                            <Button
                                variant="secondary"
                                title="Edit connection string"
                                onClick={() => dispatch(connectionStringsActions.editConnectionModalOpened(connection))}
                                disabled={isEditDisabled}
                            >
                                <Icon icon="edit" margin="m-0" />
                            </Button>
                        </ConditionalPopover>
                        <ConditionalPopover
                            conditions={[
                                {
                                    isActive: !hasWriteAccess,
                                    message: getAccessRequiredMessage(isServerWide ? "Operator" : "DatabaseAdmin"),
                                },
                                {
                                    isActive: isInheritedFromServerWide,
                                    message: (
                                        <>
                                            This connection string is managed server-wide. To edit or delete it, go to{" "}
                                            <a href={appUrl.forServerWideConnectionStrings()}>
                                                Server-Wide Connection Strings
                                            </a>
                                            .
                                        </>
                                    ),
                                },
                                {
                                    isActive: connection.usedBy?.length > 0,
                                    message: "Connection string is being used by an ongoing task",
                                },
                            ]}
                        >
                            <ButtonWithSpinner
                                variant="danger"
                                title="Delete connection string"
                                disabled={isDeleteDisabled}
                                onClick={onDelete}
                                icon="trash"
                                isSpinning={asyncDelete.loading}
                            />
                        </ConditionalPopover>
                    </RichPanelActions>
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
