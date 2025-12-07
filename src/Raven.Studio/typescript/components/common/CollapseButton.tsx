import { Icon } from "components/common/Icon";
import Button from "react-bootstrap/Button";

interface CollapseButtonProps {
    isExpanded: boolean;
    toggle: () => void;
    variant?: Button.BtnProps["variant"];
    size?: Button.BtnProps["size"];
    className?: string;
}

export default function CollapseButton({
    isExpanded,
    toggle,
    variant = "link",
    size = "xs",
    className = "text-reset",
}: CollapseButtonProps) {
    return (
        <Button variant={variant} size={size} className={className} onClick={toggle}>
            <Icon icon={isExpanded ? "collapse-vertical" : "expand-vertical"} margin="m-0" />
        </Button>
    );
}
