import { Icon } from "components/common/Icon";
import AdminLogsConfigAuditLogs from "components/pages/resources/manageServer/adminLogs/disk/settings/AdminLogsConfigAuditLogs";
import AdminLogsConfigEventListener from "components/pages/resources/manageServer/adminLogs/disk/settings/AdminLogsConfigEventListener";
import AdminLogsConfigLogs from "components/pages/resources/manageServer/adminLogs/disk/settings/AdminLogsConfigLogs";
import AdminLogsConfigMicrosoftLogs from "components/pages/resources/manageServer/adminLogs/disk/settings/AdminLogsConfigMicrosoftLogs";
import AdminLogsConfigTrafficWatch from "components/pages/resources/manageServer/adminLogs/disk/settings/AdminLogsConfigTrafficWatch";
import { adminLogsActions } from "components/pages/resources/manageServer/adminLogs/store/adminLogsSlice";
import { useAppDispatch } from "components/store";
import { useState } from "react";
import Modal from "components/common/Modal";
import Accordion from "react-bootstrap/Accordion";

type ConfigSection = "logs" | "auditLogs" | "microsoftLogs" | "trafficWatch" | "eventListener";

export default function AdminLogsDiskSettingsModal() {
    const dispatch = useAppDispatch();
    const [open, setOpen] = useState<ConfigSection>(null);

    const toggleAccordion = (id: ConfigSection) => {
        if (open === id) {
            setOpen(null);
        } else {
            setOpen(id);
        }
    };

    return (
        <Modal show size="lg" onHide={() => dispatch(adminLogsActions.isDiscSettingOpenToggled())}>
            <Modal.Header onCloseClick={() => dispatch(adminLogsActions.isDiscSettingOpenToggled())}>
                <h3>
                    <Icon icon="drive" addon="settings" />
                    Settings - logs on disk
                </h3>
            </Modal.Header>
            <Modal.Body>
                <Accordion
                    activeKey={open ?? ""}
                    onSelect={(eventKey) => toggleAccordion(eventKey as ConfigSection)}
                    className="overflow-auto d-flex flex-column gap-1"
                    style={{ maxHeight: "500px" }}
                >
                    <AdminLogsConfigLogs targetId="logs" />
                    <AdminLogsConfigAuditLogs targetId="auditLogs" />
                    <AdminLogsConfigMicrosoftLogs targetId="microsoftLogs" />
                    <AdminLogsConfigTrafficWatch targetId="trafficWatch" />
                    <AdminLogsConfigEventListener targetId="eventListener" />
                </Accordion>
            </Modal.Body>
        </Modal>
    );
}
