import { ReactNode } from "react";
import classNames from "classnames";
import Button from "react-bootstrap/Button";
import { useFormContext, useWatch } from "react-hook-form";
import moment from "moment";
import genUtils from "common/generalUtils";
import { Icon } from "components/common/Icon";
import { DistributionItem, DistributionLegend, LocationDistribution } from "components/common/LocationDistribution";
import { useDestinationTest } from "components/common/formDestinations/useDestinationTest";
import DestinationTestButton from "components/common/formDestinations/DestinationTestButton";
import ConnectionTestErrorModal from "components/common/connectionTests/ConnectionTestErrorModal";
import { DestinationType } from "components/common/formDestinations/utils/formDestinationsTypes";
import { formDestinationsUtils } from "components/common/formDestinations/utils/formDestinationsUtils";
import { cronEditorUtils } from "components/common/cronEditor/cronEditorUtils";
import { SelectOption } from "components/common/select/Select";
import { useStudioTranslation } from "hooks/useStudioTranslation";
import { EditBackupTaskFormData } from "../utils/editBackupTaskValidation";
import { editBackupTaskUtils } from "../utils/editBackupTaskUtils";
import { EditBackupTaskContext, EditBackupTaskStepId } from "../utils/editBackupTaskSteps";

interface EditBackupTaskStepSummaryProps {
    context: EditBackupTaskContext;
}

export default function EditBackupTaskStepSummary({ context }: EditBackupTaskStepSummaryProps) {
    const { watch } = useFormContext<EditBackupTaskFormData>();
    const formValues = watch();
    const t = useStudioTranslation("editBackupTask");
    const destinationsT = useStudioTranslation("formDestinations");

    const isPeriodic = !context.isManual;
    const isSnapshot = formValues.backupType === "Snapshot";
    const addedDestinationTypes = formDestinationsUtils.getAddedDestinationTypes(formValues.destinations);

    return (
        <div className="vstack gap-3">
            <SummarySection title={t("summary.backupSelection")}>
                <SummaryRow
                    label={t("summary.backupType")}
                    value={editBackupTaskUtils.getBackupTypeLabel(formValues.backupType, t)}
                />
            </SummarySection>
            <SummarySection title={t("summary.configurationSettings")} stepId="settings" context={context}>
                {isPeriodic && formValues.name && <SummaryRow label={t("settings.taskName")} value={formValues.name} />}
                {isPeriodic && (
                    <SummaryRow
                        label={t("settings.taskState")}
                        value={
                            formValues.state === "Enabled"
                                ? getLabel(editBackupTaskUtils.getTaskStateOptions(t), formValues.state)
                                : null
                        }
                        valueClassName="text-success"
                    />
                )}
                <SummaryRow
                    label={t("settings.uploadMode")}
                    value={getLabel(editBackupTaskUtils.getBackupUploadModeOptions(t), formValues.backupUploadMode)}
                />
                {isSnapshot && (
                    <>
                        <SummaryRow
                            label={t("settings.compressionAlgorithm")}
                            value={getLabel(
                                editBackupTaskUtils.getCompressionAlgorithmOptions(t),
                                formValues.snapshotCompressionAlgorithm
                            )}
                        />
                        <SummaryRow
                            label={t("settings.compressionLevel")}
                            value={getLabel(
                                editBackupTaskUtils.getCompressionLevelOptions(t),
                                formValues.snapshotCompressionLevel
                            )}
                        />
                    </>
                )}
                {isPeriodic && (
                    <SummaryRow
                        label={t("summary.responsibleNode")}
                        value={
                            formValues.isSetResponsibleNode
                                ? t(formValues.isPinResponsibleNode ? "summary.pinnedNodeValue" : "summary.nodeValue", {
                                      nodeTag: formValues.responsibleNode,
                                  })
                                : null
                        }
                    />
                )}
                <SummaryRow
                    label={t("settings.maxReadOps")}
                    value={
                        formValues.isSetMaxReadOpsPerSecond ? formValues.maxReadOpsPerSecond?.toLocaleString() : null
                    }
                />
                {isPeriodic && (
                    <SummaryRow
                        label={t("settings.retentionTime")}
                        value={
                            formValues.isSetRetentionPolicy
                                ? genUtils.formatDuration(
                                      moment.duration(formValues.minimumBackupAgeToKeepInSec, "seconds"),
                                      true
                                  )
                                : null
                        }
                    />
                )}
                {isSnapshot && (
                    <SummaryRow
                        label={t("settings.excludeIndexesSwitch")}
                        value={formValues.isSnapshotExcludeIndexes ? t("summary.enabledValue") : null}
                        valueClassName="text-success"
                    />
                )}
                <SummaryRow
                    label={t("settings.encryptionHeading")}
                    value={
                        formValues.isEncrypted
                            ? getLabel(editBackupTaskUtils.getEncryptionModeOptions(t), formValues.encryptionMode)
                            : null
                    }
                    valueClassName="text-success"
                />
            </SummarySection>
            {isPeriodic && (
                <SummarySection title={t("summary.backupSchedule")} stepId="schedule" context={context}>
                    <SummaryRow
                        label={t("schedule.fullTitle")}
                        value={
                            formValues.isFullBackupEnabled
                                ? cronEditorUtils.getDescription(formValues.fullBackupFrequency)
                                : null
                        }
                    />
                    <SummaryRow
                        label={t("schedule.incrementalTitle")}
                        value={
                            formValues.isIncrementalBackupEnabled
                                ? cronEditorUtils.getDescription(formValues.incrementalBackupFrequency)
                                : null
                        }
                    />
                </SummarySection>
            )}
            <SummarySection title={t("summary.destinations")} stepId="destinations" context={context}>
                <div className="edit-backup-task-summary-destinations">
                    <LocationDistribution>
                        <DistributionLegend>
                            <div className="top"></div>
                            {isPeriodic && <div>{t("summary.destinationState")}</div>}
                            <div>{destinationsT("authenticationMethod")}</div>
                            <div>{t("summary.testCredentials")}</div>
                        </DistributionLegend>
                        {addedDestinationTypes.map((type) => (
                            <SummaryDestination key={type} type={type} isStateVisible={isPeriodic} />
                        ))}
                    </LocationDistribution>
                </div>
            </SummarySection>
        </div>
    );
}

interface SummarySectionProps {
    title: string;
    children: ReactNode;
    stepId?: EditBackupTaskStepId;
    context?: EditBackupTaskContext;
}

function SummarySection({ title, children, stepId, context }: SummarySectionProps) {
    const commonT = useStudioTranslation("common");

    return (
        <section className="vstack gap-2" aria-label={title}>
            <div className="hstack justify-content-between">
                <div>{title}</div>
                {stepId && (
                    <Button
                        variant="link"
                        className="edit-backup-task-link md-label text-decoration-none p-0"
                        onClick={() => context.goToStep(stepId)}
                    >
                        <Icon icon="edit" />
                        {commonT("edit")}
                    </Button>
                )}
            </div>
            <div className="panel-bg-1 p-3 rounded-2 vstack gap-2">{children}</div>
        </section>
    );
}

interface SummaryRowProps {
    label: string;
    value: string | null | undefined;
    valueClassName?: string;
}

function SummaryRow({ label, value, valueClassName = "text-emphasis" }: SummaryRowProps) {
    const t = useStudioTranslation("editBackupTask");

    return (
        <div className="hstack gap-3 justify-content-between">
            <div className="text-nowrap">{label}</div>
            <div className={value ? classNames("text-end text-break", valueClassName) : "text-muted"}>
                {value || t("summary.disabledValue")}
            </div>
        </div>
    );
}

interface SummaryDestinationProps {
    type: DestinationType;
    isStateVisible: boolean;
}

function SummaryDestination({ type, isStateVisible }: SummaryDestinationProps) {
    const { control } = useFormContext<EditBackupTaskFormData>();
    const destination = useWatch({ control, name: `destinations.${type}` });
    const asyncTest = useDestinationTest(type);
    const t = useStudioTranslation("editBackupTask");
    const commonT = useStudioTranslation("common");
    const destinationsT = useStudioTranslation("formDestinations");

    const typeLabel = destinationsT(`types.${type}`);
    const isTestAvailable = formDestinationsUtils.isCredentialsTestAvailable(type, destination);

    return (
        <DistributionItem>
            <div className="top">
                <Icon icon={formDestinationsUtils.DESTINATION_TYPE_INFO[type].icon} />
                {typeLabel}
            </div>
            {isStateVisible && (
                <div className={destination.isEnabled ? "text-success" : "text-muted"}>
                    {destination.isEnabled ? t("summary.enabledValue") : t("summary.disabledValue")}
                </div>
            )}
            <div>{formDestinationsUtils.getAuthenticationMethodLabel(destination, destinationsT)}</div>
            <div className="p-1">
                {isTestAvailable ? (
                    <div className="vstack gap-1 align-items-center justify-content-center">
                        <DestinationTestButton
                            asyncTest={asyncTest}
                            size="sm"
                            className="rounded-pill"
                            title={t("summary.testCredentialsOf", { destination: typeLabel })}
                        >
                            {commonT("test")}
                        </DestinationTestButton>
                        {asyncTest.isErrorModalOpen && (
                            <ConnectionTestErrorModal
                                error={asyncTest.result.Error}
                                onClose={asyncTest.closeErrorModal}
                            />
                        )}
                    </div>
                ) : (
                    <span className="text-muted">—</span>
                )}
            </div>
        </DistributionItem>
    );
}

function getLabel<T>(options: SelectOption<T>[], value: T): string {
    return options.find((x) => x.value === value)?.label ?? null;
}
