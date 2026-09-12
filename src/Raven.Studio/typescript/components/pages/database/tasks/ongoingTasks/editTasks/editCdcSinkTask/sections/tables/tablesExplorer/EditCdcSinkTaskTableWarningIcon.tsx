import { Icon } from "components/common/Icon";
import PopoverWithHoverWrapper from "components/common/PopoverWithHoverWrapper";
import EditCdcSinkTaskWarningMessage from "components/pages/database/tasks/ongoingTasks/editTasks/editCdcSinkTask/partials/EditCdcSinkTaskWarningMessage";

interface EditCdcSinkTaskTableWarningIconProps {
    messages?: string[];
    className?: string;
}

export function EditCdcSinkTaskTableWarningIcon({ messages, className }: EditCdcSinkTaskTableWarningIconProps) {
    if (!messages?.length) {
        return null;
    }

    return (
        <PopoverWithHoverWrapper
            message={
                messages.length === 1 ? (
                    <EditCdcSinkTaskWarningMessage message={messages[0]} />
                ) : (
                    <ul className="mb-0 ps-3">
                        {messages.map((message) => (
                            <li key={message}>
                                <EditCdcSinkTaskWarningMessage message={message} />
                            </li>
                        ))}
                    </ul>
                )
            }
        >
            <Icon icon="warning" color="warning" margin="ms-1" className={className} />
        </PopoverWithHoverWrapper>
    );
}
