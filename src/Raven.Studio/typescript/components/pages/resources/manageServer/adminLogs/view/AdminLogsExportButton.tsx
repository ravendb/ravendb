import fileDownloader from "common/fileDownloader";
import { Icon } from "components/common/Icon";
import { useEventsCollector } from "components/hooks/useEventsCollector";
import { adminLogsSelectors } from "components/pages/resources/manageServer/adminLogs/store/adminLogsSlice";
import { useAppSelector } from "components/store";
import moment from "moment";
import Button from "react-bootstrap/Button";

export default function AdminLogsExportButton() {
    const eventsCollector = useEventsCollector();
    const filteredLogs = useAppSelector(adminLogsSelectors.filteredLogs);

    const exportToFile = () => {
        const fileName = "admin-log-" + moment().format("YYYY-MM-DD HH-mm") + ".json";

        fileDownloader.downloadAsJson(
            // eslint-disable-next-line @typescript-eslint/no-unused-vars
            filteredLogs.map(({ _meta: ignored, ...logWithoutMeta }) => logWithoutMeta),
            fileName
        );
    };

    return (
        <Button
            type="button"
            variant="secondary"
            onClick={() => {
                eventsCollector.reportEvent("admin-logs", "export");
                exportToFile();
            }}
        >
            <Icon icon="export" />
            Export
        </Button>
    );
}
