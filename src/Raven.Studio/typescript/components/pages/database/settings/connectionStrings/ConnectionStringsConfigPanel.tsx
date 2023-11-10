import React from "react";
import {
    RichPanel,
    RichPanelHeader,
    RichPanelInfo,
    RichPanelName,
    RichPanelActions,
} from "components/common/RichPanel";
import { Button } from "reactstrap";
import { Icon } from "components/common/Icon";

interface ConnectionStringsConfigPanelProps {
    isDatabaseAdmin: boolean;
}

export default function ConnectionStringsConfigPanel(props: ConnectionStringsConfigPanelProps) {
    const { isDatabaseAdmin } = props;

    return (
        <RichPanel className="flex-row">
            <div className="flex-grow-1">
                <RichPanelHeader>
                    <RichPanelInfo>
                        <RichPanelName>Credentials name</RichPanelName>
                    </RichPanelInfo>
                    <RichPanelActions>
                        {isDatabaseAdmin && (
                            <>
                                <Button color="secondary" title="Edit connection string">
                                    <Icon icon="edit" margin="m-0" />
                                </Button>
                                <Button color="danger" title="Delete connection string">
                                    <Icon icon="trash" margin="m-0" />
                                </Button>
                            </>
                        )}
                    </RichPanelActions>
                </RichPanelHeader>
            </div>
        </RichPanel>
    );
}
