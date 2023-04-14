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
            <div className="d-flex">
                <div>
                    <strong>Error loading data</strong>
                    {error && <div>{error}</div>}
                </div>

                <FlexGrow />
                {refresh && (
                    <Button onClick={refresh}>
                        <Icon icon="refresh" className="me-1" /> Refresh
                    </Button>
                )}
            </div>
        </Alert>
    );
}
