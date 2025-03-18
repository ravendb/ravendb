import { ConditionalPopover } from "components/common/ConditionalPopover";
import { InputItemLimit } from "components/models/common";
import genUtils from "common/generalUtils";

interface ToggleLimitBadgeProps {
    count: number;
    limit: InputItemLimit;
}

export default function ToggleLimitBadge({ count, limit }: ToggleLimitBadgeProps) {
    return (
        <ConditionalPopover
            conditions={{
                isActive: limit.message != null,
                message: limit.message,
            }}
        >
            <span className={`multi-toggle-item-count text-dark bg-${limit.badgeColor ?? "warning"}`}>
                {genUtils.formatNumberToStringFixed(count, 0)} / {genUtils.formatNumberToStringFixed(limit.value, 0)}
            </span>
        </ConditionalPopover>
    );
}
