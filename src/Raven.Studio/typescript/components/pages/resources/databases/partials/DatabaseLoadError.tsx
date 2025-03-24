import { Icon } from "components/common/Icon";
import PopoverWithHoverWrapper from "components/common/PopoverWithHoverWrapper";

export function DatabaseLoadError(props: { error: string }) {
    return (
        <PopoverWithHoverWrapper message={`Unable to load database: ${props.error}`}>
            <strong className="text-danger">
                <Icon icon="exclamation" /> Load error
            </strong>
        </PopoverWithHoverWrapper>
    );
}
