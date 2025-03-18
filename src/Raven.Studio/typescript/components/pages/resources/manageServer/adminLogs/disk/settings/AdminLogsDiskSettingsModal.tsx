import { Icon } from "components/common/Icon";
import AdminLogsConfigAuditLogs from "components/pages/resources/manageServer/adminLogs/disk/settings/AdminLogsConfigAuditLogs";
import AdminLogsConfigEventListener from "components/pages/resources/manageServer/adminLogs/disk/settings/AdminLogsConfigEventListener";
import AdminLogsConfigLogs from "components/pages/resources/manageServer/adminLogs/disk/settings/AdminLogsConfigLogs";
import AdminLogsConfigMicrosoftLogs from "components/pages/resources/manageServer/adminLogs/disk/settings/AdminLogsConfigMicrosoftLogs";
import AdminLogsConfigTrafficWatch from "components/pages/resources/manageServer/adminLogs/disk/settings/AdminLogsConfigTrafficWatch";
import { adminLogsActions } from "components/pages/resources/manageServer/adminLogs/store/adminLogsSlice";
import { useAppDispatch } from "components/store";
import { useState } from "react";
import { Accordion, CloseButton, Modal, ModalBody } from "reactstrap";

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
        <Modal isOpen wrapClassName="bs5" centered size="lg">
            <ModalBody>
                <div className="d-flex">
                    <h3>
                        <Icon icon="drive" addon="settings" />
                        Settings - logs on disk
                    </h3>
                    <CloseButton
                        className="ms-auto"
                        onClick={() => dispatch(adminLogsActions.isDiscSettingOpenToggled())}
                    />
                </div>

                <Accordion
                    open={open ?? ""}
                    toggle={toggleAccordion}
                    className="bs5 overflow-scroll vstack gap-1"
                    style={{ maxHeight: "500px" }}
                >
                    <AdminLogsConfigLogs targetId="logs" />
                    <AdminLogsConfigAuditLogs targetId="auditLogs" />
                    <AdminLogsConfigMicrosoftLogs targetId="microsoftLogs" />
                    <AdminLogsConfigTrafficWatch targetId="trafficWatch" />
                    <AdminLogsConfigEventListener targetId="eventListener" />
                </Accordion>
            </ModalBody>
        </Modal>
    );
}
