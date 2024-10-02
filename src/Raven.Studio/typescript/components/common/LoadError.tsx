import { Button } from "reactstrap";
import React from "react";
import { FlexGrow } from "components/common/FlexGrow";
import { Icon } from "./Icon";
import RichAlert from "components/common/RichAlert";

interface LoadErrorProps {
    error?: string;
    refresh?: () => void;
}

export function LoadError(props: LoadErrorProps) {
    const { error, refresh } = props;

    return (
        <RichAlert variant="danger">
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
        </RichAlert>
    );
}
