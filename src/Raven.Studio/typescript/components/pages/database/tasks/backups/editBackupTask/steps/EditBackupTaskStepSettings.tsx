import { ReactNode } from "react";
import Collapse from "react-bootstrap/Collapse";
import { useFormContext, useWatch } from "react-hook-form";
import moment from "moment";
import genUtils from "common/generalUtils";
import { FormDurationPicker, FormGroup, FormInput, FormLabel, FormSelect, FormSwitch } from "components/common/Form";
import { FormTaskResponsibleNode } from "components/common/formFields/FormTaskResponsibleNode";
import PopoverWithHoverWrapper from "components/common/PopoverWithHoverWrapper";
import OptionalLabel from "components/common/OptionalLabel";
import RichAlert from "components/common/RichAlert";
import { Icon } from "components/common/Icon";
import { StudioTrans } from "components/common/i18n/StudioTrans";
import { useStudioTranslation } from "hooks/useStudioTranslation";
import { EditBackupTaskFormData } from "../utils/editBackupTaskValidation";
import { editBackupTaskUtils } from "../utils/editBackupTaskUtils";
import { EditBackupTaskContext } from "../utils/editBackupTaskSteps";
import EditBackupTaskEncryption from "../partials/EditBackupTaskEncryption";
import { useEditBackupTaskRestrictions } from "../hooks/useEditBackupTaskRestrictions";

interface EditBackupTaskStepSettingsProps {
    context: EditBackupTaskContext;
}

export default function EditBackupTaskStepSettings({ context }: EditBackupTaskStepSettingsProps) {
    const { control } = useFormContext<EditBackupTaskFormData>();
    const [backupType, isSetMaxReadOpsPerSecond, isSetRetentionPolicy, minimumBackupAgeToKeepInSec] = useWatch({
        control,
        name: ["backupType", "isSetMaxReadOpsPerSecond", "isSetRetentionPolicy", "minimumBackupAgeToKeepInSec"],
    });
    const { hasDatabaseAdminAccess } = useEditBackupTaskRestrictions(context);
    const t = useStudioTranslation("editBackupTask");

    const isSnapshot = backupType === "Snapshot";
    const isPeriodic = !context.isManual;
    const isReadOnly = !hasDatabaseAdminAccess;

    return (
        <div>
            <h3>{t("settings.basicHeading")}</h3>
            <div className="panel-bg-1 border border-color-light rounded-2 p-3 vstack gap-2 mb-4">
                {isPeriodic && (
                    <FormGroup marginClass="m-0">
                        <FormLabel>
                            {t("settings.taskName")} <OptionalLabel />
                        </FormLabel>
                        <FormInput
                            type="text"
                            control={control}
                            name="name"
                            placeholder={t("settings.taskNamePlaceholder")}
                            autoComplete="off"
                            disabled={isReadOnly}
                        />
                    </FormGroup>
                )}
                {isPeriodic && (
                    <FormGroup marginClass="m-0">
                        <FormLabel>{t("settings.taskState")}</FormLabel>
                        <FormSelect
                            control={control}
                            name="state"
                            options={editBackupTaskUtils.getTaskStateOptions(t)}
                            isDisabled={isReadOnly}
                        />
                    </FormGroup>
                )}
                <FormGroup marginClass="m-0">
                    <FormLabel>{t("settings.uploadMode")}</FormLabel>
                    <FormSelect
                        control={control}
                        name="backupUploadMode"
                        options={editBackupTaskUtils.getBackupUploadModeOptions(t)}
                        isDisabled={isReadOnly}
                    />
                </FormGroup>
                {isSnapshot && (
                    <>
                        <FormGroup marginClass="m-0">
                            <FormLabel>{t("settings.compressionAlgorithm")}</FormLabel>
                            <FormSelect
                                control={control}
                                name="snapshotCompressionAlgorithm"
                                options={editBackupTaskUtils.getCompressionAlgorithmOptions(t)}
                                isDisabled={isReadOnly}
                            />
                        </FormGroup>
                        <FormGroup marginClass="m-0">
                            <FormLabel>{t("settings.compressionLevel")}</FormLabel>
                            <FormSelect
                                control={control}
                                name="snapshotCompressionLevel"
                                options={editBackupTaskUtils.getCompressionLevelOptions(t)}
                                isDisabled={isReadOnly}
                            />
                        </FormGroup>
                    </>
                )}
            </div>

            <h3>{t("settings.additionalHeading")}</h3>
            <div className="panel-bg-1 border border-color-light rounded-2 p-3 mb-4 edit-backup-task-settings-list">
                {isPeriodic && (
                    <SettingRow>
                        <FormTaskResponsibleNode
                            control={control}
                            isSetName="isSetResponsibleNode"
                            nodeName="responsibleNode"
                            isPinName="isPinResponsibleNode"
                            isDisabled={isReadOnly}
                        />
                    </SettingRow>
                )}
                <SettingRow>
                    <FormSwitch control={control} name="isSetMaxReadOpsPerSecond" disabled={isReadOnly}>
                        {t("settings.maxReadOpsSwitch")}
                        <InfoPopover message={t("settings.maxReadOpsInfo")} />
                    </FormSwitch>
                    <Collapse in={isSetMaxReadOpsPerSecond} mountOnEnter unmountOnExit>
                        <div className="mt-2">
                            <FormGroup marginClass="m-0" className="vstack gap-1">
                                <FormLabel className="mb-0">{t("settings.maxReadOps")}</FormLabel>
                                <FormInput
                                    type="number"
                                    control={control}
                                    name="maxReadOpsPerSecond"
                                    placeholder={t("settings.maxReadOpsPlaceholder")}
                                    aria-label={t("settings.maxReadOps")}
                                    disabled={isReadOnly}
                                />
                            </FormGroup>
                        </div>
                    </Collapse>
                </SettingRow>
                {isPeriodic && (
                    <SettingRow>
                        <FormSwitch control={control} name="isSetRetentionPolicy" disabled={isReadOnly}>
                            {t("settings.retentionSwitch")}
                            <InfoPopover message={t("settings.retentionInfo")} />
                        </FormSwitch>
                        <Collapse in={isSetRetentionPolicy} mountOnEnter unmountOnExit>
                            <div className="mt-2 vstack gap-2">
                                <div className="vstack gap-1">
                                    <FormLabel className="mb-0">{t("settings.retentionTime")}</FormLabel>
                                    <FormDurationPicker
                                        control={control}
                                        name="minimumBackupAgeToKeepInSec"
                                        showSeconds={false}
                                        aria-label={t("settings.retentionTime")}
                                        disabled={isReadOnly}
                                    />
                                </div>
                                {minimumBackupAgeToKeepInSec > 0 && (
                                    <RichAlert variant="info">
                                        <StudioTrans
                                            ns="editBackupTask"
                                            i18nKey="settings.retentionAlert"
                                            components={{ strong: <strong /> }}
                                            options={{
                                                duration: genUtils.formatDuration(
                                                    moment.duration(minimumBackupAgeToKeepInSec, "seconds"),
                                                    true
                                                ),
                                            }}
                                        />
                                    </RichAlert>
                                )}
                            </div>
                        </Collapse>
                    </SettingRow>
                )}
                {isSnapshot && (
                    <SettingRow>
                        <FormSwitch control={control} name="isSnapshotExcludeIndexes" disabled={isReadOnly}>
                            {t("settings.excludeIndexesSwitch")}
                            <InfoPopover message={t("settings.excludeIndexesInfo")} />
                        </FormSwitch>
                    </SettingRow>
                )}
            </div>

            <h3>{t("settings.encryptionHeading")}</h3>
            <EditBackupTaskEncryption isDisabled={isReadOnly} />
        </div>
    );
}

function SettingRow({ children }: { children: ReactNode }) {
    return <div className="edit-backup-task-settings-row">{children}</div>;
}

function InfoPopover({ message }: { message: string }) {
    return (
        <PopoverWithHoverWrapper message={message}>
            <Icon icon="info" color="info" margin="ms-1" />
        </PopoverWithHoverWrapper>
    );
}
