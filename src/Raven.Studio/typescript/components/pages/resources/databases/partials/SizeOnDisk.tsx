import { DatabaseLocalInfo } from "components/models/databases";
import genUtils from "common/generalUtils";
import useUniqueId from "components/hooks/useUniqueId";
import OverlayTrigger from "react-bootstrap/OverlayTrigger";
import Tooltip from "react-bootstrap/Tooltip";

export function SizeOnDisk(props: { info: DatabaseLocalInfo }) {
    const { info } = props;

    const tooltipId = useUniqueId("size-on-disk-tooltip");

    if (!info) {
        return null;
    }
    const tempBufferSize = info.tempBuffersSize?.SizeInBytes ?? 0;
    const totalSize = info.totalSize?.SizeInBytes ?? 0;
    const grandTotalSize = tempBufferSize + totalSize;

    return (
        <div>
            <OverlayTrigger
                overlay={
                    <Tooltip id={tooltipId}>
                        Data: <strong>{genUtils.formatBytesToSize(totalSize)}</strong>
                        <br />
                        Temp: <strong>{genUtils.formatBytesToSize(tempBufferSize)}</strong>
                        <br />
                        Total: <strong>{genUtils.formatBytesToSize(grandTotalSize)}</strong>
                    </Tooltip>
                }
            >
                <div>{genUtils.formatBytesToSize(grandTotalSize)}</div>
            </OverlayTrigger>
        </div>
    );
}
