import React from "react";
import { Button, ButtonProps, Spinner } from "reactstrap";
import { Icon } from "./Icon";

interface RunScriptButtonProps extends ButtonProps {
    isSpinning?: boolean;
}

export default function RunScriptButton(props: RunScriptButtonProps) {
    const { onClick, isSpinning, disabled } = props;

    return (
        <div className="run-script-button">
            <Button color="primary" size="lg" className="px-4 py-2" onClick={onClick} disabled={disabled || isSpinning}>
                {isSpinning ? <Spinner /> : <Icon icon="play" className="fs-1 d-inline-block" margin="mb-2" />}
                <div className="kbd">
                    <kbd>ctrl</kbd> <strong>+</strong> <kbd>enter</kbd>
                </div>
            </Button>
        </div>
    );
}
