import { Icon } from "components/common/Icon";
import PopoverWithHoverWrapper from "components/common/PopoverWithHoverWrapper";

export default function AdminLogsPersistInfoIcon() {
    return (
        <PopoverWithHoverWrapper message="If not saved, the minimum level will reset after a server restart.">
            <Icon icon="info" color="info" margin="ms-1" />
        </PopoverWithHoverWrapper>
    );
}
