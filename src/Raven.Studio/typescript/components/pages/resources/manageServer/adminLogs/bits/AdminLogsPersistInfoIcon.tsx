import { Icon } from "components/common/Icon";
import useUniqueId from "components/hooks/useUniqueId";
import { UncontrolledPopover } from "reactstrap";

export default function AdminLogsPersistInfoIcon() {
    const id = useUniqueId("persist-info-");

    return (
        <>
            <Icon icon="info" color="info" margin="ms-1" id={id} />
            <UncontrolledPopover target={id} trigger="hover" className="bs5">
                <div className="p-3">If not saved, the minimum level will reset after a server restart.</div>
            </UncontrolledPopover>
        </>
    );
}
