import { Alert, Button } from "reactstrap";
import React from "react";
import { FlexGrow } from "components/common/FlexGrow";
import { Icon } from "./Icon";

interface LoadErrorProps {
    error?: string;
    refresh?: () => void;
}

export function LoadError(props: LoadErrorProps) {
    const { error, refresh } = props;

    return (
        <Alert color="danger">
            <div className="d-flex gap-1">
                <div>
                    <strong>Error loading data</strong>
                    {error && <div>{error}</div>}
                </div>

                <FlexGrow />
                {refresh && (
                    <Button onClick={refresh}>
                        <Icon icon="refresh" /> Refresh
                    </Button>
                )}
            </div>
        </Alert>
    );
}
