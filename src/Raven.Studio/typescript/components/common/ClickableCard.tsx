import classNames from "classnames";
import IconName from "typings/server/icons";
import { Icon } from "./Icon";

interface ClickableCardProps {
    icon: IconName;
    title: string;
    description: string;
    isSelected?: boolean;
    className?: string;
    isDisabled?: boolean;
    onClick: () => void;
}

export default function ClickableCard({
    description,
    icon,
    onClick,
    title,
    isSelected,
    className,
    isDisabled,
}: ClickableCardProps) {
    return (
        <div
            className={classNames(
                "border rounded p-2 cursor-pointer hover-filter",
                {
                    "bg-faded-primary border-primary": isSelected,
                },
                {
                    "border-secondary": !isSelected,
                },
                {
                    "opacity-50 cursor-not-allowed": isDisabled,
                },
                className
            )}
            onClick={isDisabled ? undefined : onClick}
        >
            <div className="text-emphasis hstack gap-2">
                <div>
                    <Icon icon={icon} margin="mx-1" />
                </div>
                <div className="flex-grow">
                    <div className="fw-semibold">{title}</div>
                    <div>{description}</div>
                </div>
            </div>
        </div>
    );
}
