import classNames from "classnames";
import CloseButton from "react-bootstrap/CloseButton";
import { Icon } from "components/common/Icon";
import IconName from "../../../typings/server/icons";
import Alert, { AlertProps } from "react-bootstrap/Alert";
import Button from "react-bootstrap/Button";
import copyToClipboard from "common/copyToClipboard";

interface RichAlertProps extends AlertProps {
    icon?: IconName;
    iconAddon?: IconName;
    title?: string;
    color?: never;
    onCancel?: () => void;
    variant: (typeof richAlertColors)[number];
    childrenClassName?: string;
    copyText?: string;
    copyTextSuccessMessage?: string;
}

const defaultIcons: { [key: string]: IconName } = {
    info: "info",
    danger: "danger",
    warning: "warning",
    success: "check",
};

export const richAlertColors = [
    "primary",
    "secondary",
    "success",
    "warning",
    "danger",
    "info",
    "progress",
    "node",
    "shard",
    "dark",
    "light",
] as const;

export function RichAlert({
    className,
    variant,
    children,
    icon,
    iconAddon,
    title,
    childrenClassName,
    copyText,
    copyTextSuccessMessage,
    onCancel,
    ...rest
}: RichAlertProps) {
    const renderAlertIcon = icon ?? defaultIcons[variant] ?? "terms";

    const handleCopyToClipboard = () => {
        copyToClipboard.copy(copyText, copyTextSuccessMessage);
    };

    return (
        <Alert
            variant={variant}
            className={classNames("position-relative", title ? "vstack" : "hstack gap-2", className)}
            {...rest}
        >
            {copyText && (
                <Button
                    onClick={handleCopyToClipboard}
                    className="position-absolute top-0 end-0 m-2 hover-filter"
                    variant="link"
                >
                    <Icon icon="copy-to-clipboard" margin="m-0" color="muted" />
                </Button>
            )}
            {title ? (
                <h3 className="hstack mb-1 gap-1">
                    <Icon icon={renderAlertIcon} addon={iconAddon} margin="m-0" className="title-icon" /> {title}
                </h3>
            ) : (
                <Icon icon={renderAlertIcon} addon={iconAddon} margin="m-0" className="title-icon fs-3" />
            )}
            <div className={classNames("w-100", childrenClassName)}>{children}</div>
            {onCancel && <CloseButton className="position-absolute end-0 top-0" onClick={onCancel} />}
        </Alert>
    );
}

export default RichAlert;
