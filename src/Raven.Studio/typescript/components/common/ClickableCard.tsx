import classNames from "classnames";
import IconName from "typings/server/icons";
import { Icon } from "./Icon";

interface ClickableCardProps {
    icon: IconName;
    title: string;
    description: string;
    isSelected: boolean;
    className?: string;
    onClick: () => void;
}

export default function ClickableCard({
    description,
    icon,
    onClick,
    title,
    isSelected,
    className,
}: ClickableCardProps) {
    return (
        <div
            className={classNames(
                "border rounded p-2 cursor-pointer",
                {
                    "bg-faded-primary border-primary": isSelected,
                },
                {
                    "border-secondary": !isSelected,
                },
                className
            )}
            onClick={onClick}
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
