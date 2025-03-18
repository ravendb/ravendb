import RichAlert from "components/common/RichAlert";
import {
    adminLogsSelectors,
    adminLogsActions,
} from "components/pages/resources/manageServer/adminLogs/store/adminLogsSlice";
import { useAppDispatch, useAppSelector } from "components/store";
import Button from "react-bootstrap/Button";

export default function AdminLogsBufferAlert() {
    const dispatch = useAppDispatch();

    const isBufferFullAlertOpen = useAppSelector(adminLogsSelectors.isBufferFullAlertOpen);

    if (!isBufferFullAlertOpen) {
        return null;
    }

    return (
        <RichAlert
            variant="danger"
            style={{
                position: "sticky",
                zIndex: 1,
                top: 15,
                left: 0,
                right: 0,
                margin: "auto",
                width: "fit-content",
            }}
            onCancel={() => dispatch(adminLogsActions.isBufferFullAlertOpenSet(false))}
        >
            Log buffer is full. Either increase buffer size in &apos;Display settings&apos; or{" "}
            <Button variant="link" onClick={() => dispatch(adminLogsActions.logsSet([]))} className="p-0">
                clear all entries
            </Button>
            .
        </RichAlert>
    );
}
