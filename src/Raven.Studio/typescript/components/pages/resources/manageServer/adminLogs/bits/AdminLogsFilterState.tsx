import { ConditionalPopover } from "components/common/ConditionalPopover";
import { Icon } from "components/common/Icon";

export default function AdminLogsFilterState({ isActive }: { isActive: boolean }) {
    return (
        <ConditionalPopover
            conditions={[
                {
                    isActive,
                    message: "There are active filters",
                },
                {
                    isActive: !isActive,
                    message: "No active filters",
                },
            ]}
        >
            <Icon icon="filter" color={isActive ? "primary" : "muted"} />
        </ConditionalPopover>
    );
}
