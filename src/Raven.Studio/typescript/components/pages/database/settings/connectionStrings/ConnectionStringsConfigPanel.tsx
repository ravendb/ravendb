import React from "react";
import {
    RichPanel,
    RichPanelHeader,
    RichPanelInfo,
    RichPanelName,
    RichPanelActions,
} from "components/common/RichPanel";
import { Button, UncontrolledTooltip } from "reactstrap";
import { Icon } from "components/common/Icon";
import { Connection } from "./connectionStringsTypes";
import { accessManagerSelectors } from "components/common/shell/accessManagerSlice";
import { useAppSelector } from "components/store";
import database from "models/resources/database";
import { useAsyncCallback } from "react-async-hook";
import ButtonWithSpinner from "components/common/ButtonWithSpinner";
import { useServices } from "components/hooks/useServices";
import { connectionStringsActions } from "./store/connectionStringsSlice";
import { useDispatch } from "react-redux";
import useConfirm from "components/common/ConfirmDialog";

interface ConnectionStringsPanelProps {
    db: database;
    connection: Connection;
}

export default function ConnectionStringsPanel(props: ConnectionStringsPanelProps) {
    const { db, connection } = props;

    const confirm = useConfirm();
    const dispatch = useDispatch();
    const { databasesService } = useServices();

    const isDatabaseAdmin =
        useAppSelector(accessManagerSelectors.effectiveDatabaseAccessLevel(db.name)) === "DatabaseAdmin";

    const asyncDelete = useAsyncCallback(async () => {
        await databasesService.deleteConnectionString(db, getDtoEtlType(connection.Type), connection.Name);

        dispatch(
            connectionStringsActions.deleteConnection({
                type: connection.Type,
                name: connection.Name,
            })
        );
    });

    const onDelete = async () => {
        const isConfirmed = await confirm({
            title: (
                <span>
                    Delete <strong>{connection.Name}</strong> connection string?
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

    const deleteButtonId = "delete-" + connection.Type + "_" + connection.Name;
    const isDeleteDisabled = connection.UsedByTasks?.length > 0;

    return (
        <RichPanel className="flex-row">
            <div className="flex-grow-1">
                <RichPanelHeader>
                    <RichPanelInfo>
                        <RichPanelName>{connection.Name}</RichPanelName>
                    </RichPanelInfo>
                    {isDatabaseAdmin && (
                        <RichPanelActions>
                            <Button
                                color="secondary"
                                title="Edit connection string"
                                onClick={() => dispatch(connectionStringsActions.openEditConnectionModal(connection))}
                            >
                                <Icon icon="edit" margin="m-0" />
                            </Button>
                            <div id={deleteButtonId}>
                                <ButtonWithSpinner
                                    color="danger"
                                    title="Delete connection string"
                                    disabled={isDeleteDisabled}
                                    onClick={onDelete}
                                    icon="trash"
                                    isSpinning={asyncDelete.loading}
                                />
                            </div>
                            {isDeleteDisabled && (
                                <UncontrolledTooltip target={deleteButtonId}>
                                    Connection string is being used by an ongoing task
                                </UncontrolledTooltip>
                            )}
                        </RichPanelActions>
                    )}
                </RichPanelHeader>
            </div>
        </RichPanel>
    );
}

function getDtoEtlType(type: StudioEtlType): Raven.Client.Documents.Operations.ETL.EtlType {
    switch (type) {
        case "Kafka":
        case "RabbitMQ":
            return "Queue";
        default:
            return type;
    }
}
