import AdminLogsConfigTableValue from "components/pages/resources/manageServer/adminLogs/bits/AdminLogsConfigTableValue";
import { adminLogsSelectors } from "components/pages/resources/manageServer/adminLogs/store/adminLogsSlice";
import { useAppSelector } from "components/store";
import { AccordionItem, AccordionBody, AccordionHeader, Table, UncontrolledPopover } from "reactstrap";
import { Icon } from "components/common/Icon";

export default function AdminLogsConfigAuditLogs({ targetId }: { targetId: string }) {
    const config = useAppSelector(adminLogsSelectors.configs).adminLogsConfig.AuditLogs;

    return (
        <AccordionItem className="p-1 rounded-3">
            <AccordionHeader targetId={targetId}>Audit logs</AccordionHeader>
            <AccordionBody accordionId={targetId}>
                <h5 className="text-center text-muted text-uppercase">
                    Read-only
                    <span id="read-only-tooltip-for-audit-logs">
                        <Icon icon="info" color="info" margin="ms-1" />
                    </span>
                    <UncontrolledPopover target="read-only-tooltip-for-audit-logs" trigger="hover" className="bs5">
                        <div className="p-3">
                            These settings are not editable here but can be configured through the server configuration.
                        </div>
                    </UncontrolledPopover>
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
            </AccordionBody>
        </AccordionItem>
    );
}
