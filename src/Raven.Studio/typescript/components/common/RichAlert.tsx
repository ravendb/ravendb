/* eslint-disable local-rules/no-reactstrap-alert */
import classNames from "classnames";
import { Alert, AlertProps } from "reactstrap";
import { Icon } from "components/common/Icon";
import IconName from "../../../typings/server/icons";

interface RichAlertProps extends AlertProps {
    icon?: IconName;
    iconAddon?: IconName;
    title?: string;
}

const defaultIcons: { [key: string]: IconName } = {
    info: "info",
    danger: "danger",
    warning: "warning",
    success: "check",
};

export function RichAlert({ className, variant, children, icon, iconAddon, title, ...rest }: RichAlertProps) {
    const renderAlertIcon = icon ?? defaultIcons[variant] ?? "terms";

    return (
        <Alert color={variant} className={classNames(title ? "vstack" : "hstack gap-2", className)} {...rest}>
            {title ? (
                <h3 className="hstack mb-1 gap-1">
                    <Icon icon={renderAlertIcon} addon={iconAddon} margin="m-0" className="title-icon" /> {title}
                </h3>
            ) : (
                <Icon icon={renderAlertIcon} addon={iconAddon} margin="m-0" className="title-icon fs-3" />
            )}
            <div>{children}</div>
        </Alert>
    );
}

export default RichAlert;
