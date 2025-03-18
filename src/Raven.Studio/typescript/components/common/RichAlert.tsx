/* eslint-disable local-rules/no-reactstrap-alert */
import classNames from "classnames";
import { CloseButton } from "reactstrap";
import { Icon } from "components/common/Icon";
import IconName from "../../../typings/server/icons";
import Alert, { AlertProps } from "react-bootstrap/Alert";

interface RichAlertProps extends AlertProps {
    icon?: IconName;
    iconAddon?: IconName;
    title?: string;
    color?: never;
    onCancel?: () => void;
    variant: (typeof richAlertColors)[number];
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

export function RichAlert({ className, variant, children, icon, iconAddon, title, onCancel, ...rest }: RichAlertProps) {
    const renderAlertIcon = icon ?? defaultIcons[variant] ?? "terms";

    return (
        <Alert variant={variant} className={classNames(title ? "vstack" : "hstack gap-2", className)} {...rest}>
            {title ? (
                <h3 className="hstack mb-1 gap-1">
                    <Icon icon={renderAlertIcon} addon={iconAddon} margin="m-0" className="title-icon" /> {title}
                </h3>
            ) : (
                <Icon icon={renderAlertIcon} addon={iconAddon} margin="m-0" className="title-icon fs-3" />
            )}
            <div className="w-100">{children}</div>
            {onCancel && (
                <CloseButton
                    className="pt-0"
                    onClick={onCancel}
                    style={{
                        position: "absolute",
                        top: 0,
                        right: 0,
                    }}
                />
            )}
        </Alert>
    );
}

export default RichAlert;
