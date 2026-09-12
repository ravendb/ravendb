import React from "react";
import Button from "react-bootstrap/Button";
import { Icon } from "components/common/Icon";

interface LoadButtonProps {
    onSelect: () => void;
}

export default function LoadButton({ onSelect }: LoadButtonProps) {
    return (
        <Button variant="link" className="text-emphasis" title="Load into editor" onClick={onSelect}>
            <Icon icon="arrow-left" margin="me-1" />
            Load
        </Button>
    );
}
