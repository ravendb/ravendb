import "./SelectableCard.scss";
import classNames from "classnames";
import { Icon } from "components/common/Icon";
import IconName from "typings/server/icons";
import PopoverWithHoverWrapper from "components/common/PopoverWithHoverWrapper";
import { ReactNode } from "react";
import { useStudioTranslation } from "hooks/useStudioTranslation";

interface SelectableCardProps {
    icon: IconName;
    addon?: IconName;
    title: string;
    description: ReactNode;
    isSelected: boolean;
    popoverMessage?: string | ReactNode;
    onClick: () => void;
    className?: string;
    isDisabled?: boolean;
    badge?: ReactNode;
}

export default function SelectableCard({
    className,
    description,
    popoverMessage,
    addon,
    icon,
    onClick,
    title,
    isDisabled,
    isSelected,
    badge,
}: SelectableCardProps) {
    const commonT = useStudioTranslation("common");

    const select = () => {
        if (!isDisabled) {
            onClick();
        }
    };

    return (
        <div
            className={classNames("selectable-card border rounded p-4", className, {
                "bg-faded-primary border-primary active": isSelected,
                "border-secondary": !isSelected,
                "item-disabled pe-none": isDisabled,
            })}
            onClick={select}
            role="radio"
            aria-checked={isSelected}
            aria-disabled={isDisabled}
            aria-label={title}
            tabIndex={isDisabled ? -1 : 0}
            onKeyDown={(event) => {
                if (event.key === "Enter" || event.key === " ") {
                    event.preventDefault();
                    select();
                }
            }}
        >
            <div className="text-emphasis hstack gap-4">
                <div>
                    <Icon icon={icon} addon={addon} margin="m-0" style={{ fontSize: 24 }} />
                </div>
                <div className="flex-grow">
                    <h4 className="mb-0 text-emphasis">
                        {title}
                        {badge}
                    </h4>
                    <span className="text-muted">{description}</span>
                </div>
                {popoverMessage && (
                    <PopoverWithHoverWrapper message={popoverMessage}>
                        <div style={{ minWidth: "120px" }} className="md-label text-right mb-0">
                            <Icon icon="info" />
                            {commonT("whenToUse")}
                        </div>
                    </PopoverWithHoverWrapper>
                )}
            </div>
        </div>
    );
}
