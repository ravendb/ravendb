import { DatabaseLocalInfo } from "components/models/databases";
import genUtils from "common/generalUtils";
import PopoverWithHoverWrapper from "components/common/PopoverWithHoverWrapper";

export function SizeOnDisk(props: { info: DatabaseLocalInfo }) {
    const { info } = props;
    if (!info) {
        return null;
    }

    const tempBufferSize = info.tempBuffersSize?.SizeInBytes ?? 0;
    const physicalSize = info.totalPhysicalSize?.SizeInBytes ?? 0;
    const allocatedSize = info.totalAllocatedSize?.SizeInBytes ?? 0;
    const grandTotalSize = tempBufferSize + physicalSize;

    return (
        <div>
            <PopoverWithHoverWrapper
                message={
                    <>
                        Data (on disk): <strong>{genUtils.formatBytesToSize(physicalSize)}</strong>
                        <br />
                        Data (allocated): <strong>{genUtils.formatBytesToSize(allocatedSize)}</strong>
                        <br />
                        Temp: <strong>{genUtils.formatBytesToSize(tempBufferSize)}</strong>
                        <br />
                        Total: <strong>{genUtils.formatBytesToSize(grandTotalSize)}</strong>
                    </>
                }
            >
                {genUtils.formatBytesToSize(grandTotalSize)}
            </PopoverWithHoverWrapper>
        </div>
    );
}
