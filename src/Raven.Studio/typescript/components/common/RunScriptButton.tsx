import React from "react";
import Spinner from "react-bootstrap/Spinner";
import { Icon } from "./Icon";
import Button from "react-bootstrap/Button";

interface RunScriptButtonProps extends Button.BtnProps {
    isSpinning?: boolean;
}

export default function RunScriptButton(props: RunScriptButtonProps) {
    const { onClick, isSpinning, disabled } = props;

    return (
        <div className="run-script-button">
            <Button
                variant="primary"
                size="lg"
                className="px-4 py-2"
                onClick={onClick}
                disabled={disabled || isSpinning}
            >
                {isSpinning ? <Spinner /> : <Icon icon="play" className="fs-1 d-inline-block" margin="mb-2" />}
                <div className="kbd">
                    <kbd>ctrl</kbd> <strong>+</strong> <kbd>enter</kbd>
                </div>
            </Button>
        </div>
    );
}
