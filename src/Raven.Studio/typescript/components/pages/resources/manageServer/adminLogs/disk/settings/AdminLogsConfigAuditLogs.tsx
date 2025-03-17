import AdminLogsConfigTableValue from "components/pages/resources/manageServer/adminLogs/bits/AdminLogsConfigTableValue";
import { adminLogsSelectors } from "components/pages/resources/manageServer/adminLogs/store/adminLogsSlice";
import { useAppSelector } from "components/store";
import Table from "react-bootstrap/Table";
import { Icon } from "components/common/Icon";
import PopoverWithHoverWrapper from "components/common/PopoverWithHoverWrapper";
import Accordion from "react-bootstrap/Accordion";

export default function AdminLogsConfigAuditLogs({ targetId }: { targetId: string }) {
    const config = useAppSelector(adminLogsSelectors.configs).adminLogsConfig.AuditLogs;

    return (
        <Accordion.Item eventKey={targetId} className="p-1 rounded-3">
            <Accordion.Header>Audit logs</Accordion.Header>
            <Accordion.Body>
                <h5 className="text-center text-muted text-uppercase">
                    Read-only
                    <PopoverWithHoverWrapper message="These settings are not editable here but can be configured through the server configuration.">
                        <Icon icon="info" color="info" margin="ms-1" />
                    </PopoverWithHoverWrapper>
                </h5>
                <Table>
                    <tbody>
                        <tr>
                            <td>Path</td>
                            <td>
                                <AdminLogsConfigTableValue value={config.Path} />
                            </td>
                        </tr>
                        <tr>
                            <td>Level</td>
                            <td>
                                <AdminLogsConfigTableValue value={config.Level} />
                            </td>
                        </tr>
                        <tr>
                            <td>Archive Above Size</td>
                            <td>
                                <AdminLogsConfigTableValue
                                    value={`${config.ArchiveAboveSizeInMb?.toLocaleString()} MB`}
                                />
                            </td>
                        </tr>
                        <tr>
                            <td>Maximum Archived Days</td>
                            <td>
                                <AdminLogsConfigTableValue value={config.MaxArchiveDays} />
                            </td>
                        </tr>
                        <tr>
                            <td>Maximum Archived Files</td>
                            <td>
                                <AdminLogsConfigTableValue value={config.MaxArchiveFiles} />
                            </td>
                        </tr>
                        <tr>
                            <td>Archive File Compression</td>
                            <td>
                                <AdminLogsConfigTableValue value={config.EnableArchiveFileCompression} />
                            </td>
                        </tr>
                    </tbody>
                </Table>
            </Accordion.Body>
        </Accordion.Item>
    );
}
