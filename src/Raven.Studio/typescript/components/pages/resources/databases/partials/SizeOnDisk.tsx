import { DatabaseLocalInfo } from "components/models/databases";
import React, { useRef } from "react";
import genUtils from "common/generalUtils";
import { UncontrolledTooltip } from "reactstrap";

export function SizeOnDisk(props: { info: DatabaseLocalInfo }) {
    const { info } = props;

    const divRef = useRef<HTMLDivElement>();

    if (!info) {
        return null;
    }
    const tempBufferSize = info.tempBuffersSize?.SizeInBytes ?? 0;
    const totalSize = info.totalSize?.SizeInBytes ?? 0;
    const grandTotalSize = tempBufferSize + totalSize;

    return (
        <div>
            <div ref={divRef}>{genUtils.formatBytesToSize(grandTotalSize)}</div>
            {divRef.current && (
                <UncontrolledTooltip target={divRef.current}>
                    Data: <strong>{genUtils.formatBytesToSize(totalSize)}</strong>
                    <br />
                    Temp: <strong>{genUtils.formatBytesToSize(tempBufferSize)}</strong>
                    <br />
                    Total: <strong>{genUtils.formatBytesToSize(grandTotalSize)}</strong>
                </UncontrolledTooltip>
            )}
        </div>
    );
}
