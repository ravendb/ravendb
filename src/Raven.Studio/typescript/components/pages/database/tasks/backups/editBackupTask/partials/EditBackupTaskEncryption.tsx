import { ChangeEvent } from "react";
import Button from "react-bootstrap/Button";
import ButtonGroup from "react-bootstrap/ButtonGroup";
import Dropdown from "react-bootstrap/Dropdown";
import Form from "react-bootstrap/Form";
import InputGroup from "react-bootstrap/InputGroup";
import Spinner from "react-bootstrap/Spinner";
import { useController, useFormContext, useWatch } from "react-hook-form";
import copyToClipboard from "common/copyToClipboard";
import fileDownloader from "common/fileDownloader";
import {
    FormCheckbox,
    FormGroup,
    FormLabel,
    FormSelect,
    FormSwitch,
    FormValidationMessage,
} from "components/common/Form";
import { ConditionalPopover } from "components/common/ConditionalPopover";
import LicenseRestrictedBadge from "components/common/LicenseRestrictedBadge";
import RichAlert from "components/common/RichAlert";
import { Icon } from "components/common/Icon";
import { useEncryptionKeyGenerator } from "components/hooks/useEncryptionKeyGenerator";
import useBoolean from "components/hooks/useBoolean";
import { encryptionKeyQrCodeUtils } from "components/utils/encryptionKeyQrCodeUtils";
import { useAppSelector } from "components/store";
import { databaseSelectors } from "components/common/shell/databaseSliceSelectors";
import { licenseSelectors } from "components/common/shell/licenseSlice";
import { useStudioTranslation } from "hooks/useStudioTranslation";
import { EditBackupTaskFormData } from "../utils/editBackupTaskValidation";
import { editBackupTaskUtils } from "../utils/editBackupTaskUtils";
import EditBackupTaskEncryptionKeyQrCodeModal from "./EditBackupTaskEncryptionKeyQrCodeModal";

interface EditBackupTaskEncryptionProps {
    isDisabled: boolean;
}

export default function EditBackupTaskEncryption({ isDisabled }: EditBackupTaskEncryptionProps) {
    const { control, formState } = useFormContext<EditBackupTaskFormData>();
    const unencryptedBackupError = formState.errors.isUnencryptedBackupAllowed?.message;
    const [backupType, isEncrypted, encryptionMode] = useWatch({
        control,
        name: ["backupType", "isEncrypted", "encryptionMode"],
    });

    const db = useAppSelector(databaseSelectors.activeDatabase);
    const hasEncryptedBackups = useAppSelector(licenseSelectors.statusValue("HasEncryptedBackups"));
    const t = useStudioTranslation("editBackupTask");

    const rules = editBackupTaskUtils.getEncryptionRules(backupType, db.isEncrypted, t);
    const isToggleDisabled = isDisabled || rules.isToggleLocked || (!hasEncryptedBackups && !isEncrypted);

    return (
        <div className="panel-bg-1 border border-color-light rounded-2 p-3 vstack gap-3">
            <div className="hstack gap-2">
                <ConditionalPopover
                    conditions={[
                        {
                            isActive: !hasEncryptedBackups,
                            message: t("license.noEncryptedBackups"),
                        },
                        { isActive: rules.isToggleLocked, message: rules.lockReason },
                    ]}
                >
                    <FormSwitch control={control} name="isEncrypted" disabled={isToggleDisabled}>
                        {t("encryption.encryptSwitch")}
                    </FormSwitch>
                </ConditionalPopover>
                {!hasEncryptedBackups && <LicenseRestrictedBadge licenseRequired="Professional +" />}
            </div>

            {!isEncrypted && db.isEncrypted && backupType === "Backup" && (
                <>
                    <RichAlert variant="warning">{t("encryption.unencryptedWarning")}</RichAlert>
                    <div>
                        <FormCheckbox control={control} name="isUnencryptedBackupAllowed" disabled={isDisabled}>
                            {t("encryption.allowUnencrypted")}
                        </FormCheckbox>
                        {unencryptedBackupError && (
                            <FormValidationMessage>{unencryptedBackupError}</FormValidationMessage>
                        )}
                    </div>
                </>
            )}

            {isEncrypted && (
                <FormGroup marginClass="m-0">
                    <FormLabel>{t("encryption.keyMode")}</FormLabel>
                    <ConditionalPopover
                        className="w-100"
                        conditions={{ isActive: rules.isModeLocked, message: rules.lockReason }}
                    >
                        <FormSelect
                            className="w-100"
                            control={control}
                            name="encryptionMode"
                            options={editBackupTaskUtils.getEncryptionModeOptions(t)}
                            isDisabled={isDisabled || rules.isModeLocked}
                            aria-label={t("encryption.keyMode")}
                        />
                    </ConditionalPopover>
                </FormGroup>
            )}

            {isEncrypted && encryptionMode === "UseProvidedKey" && <BackupEncryptionKeyField isDisabled={isDisabled} />}
        </div>
    );
}

function BackupEncryptionKeyField({ isDisabled }: EditBackupTaskEncryptionProps) {
    const { control, setValue } = useFormContext<EditBackupTaskFormData>();
    const originalEncryptionKey = useWatch({ control, name: "originalEncryptionKey" });
    const {
        field: { value: encryptionKey, onChange, onBlur, ref },
        fieldState: { invalid, error },
    } = useController({ control, name: "encryptionKey" });

    const databaseName = useAppSelector(databaseSelectors.activeDatabaseName);
    const { value: isQrCodeVisible, setTrue: showQrCode, setFalse: hideQrCode } = useBoolean(false);
    const t = useStudioTranslation("editBackupTask");
    const commonT = useStudioTranslation("common");

    const { isGenerating, regenerate } = useEncryptionKeyGenerator(encryptionKey, (key) => {
        setValue("encryptionKey", key, { shouldDirty: true, shouldValidate: true });
        setValue("isEncryptionKeySaved", false, { shouldDirty: true });
    });

    const isKeyValid = !!encryptionKey && !invalid;
    const isOriginalKey = !!originalEncryptionKey && encryptionKey === originalEncryptionKey;
    const isNewKey = !!encryptionKey && !isOriginalKey;
    const keyText = editBackupTaskUtils.getEncryptionKeyText(databaseName, encryptionKey, t);
    const fileName = editBackupTaskUtils.getEncryptionKeyFileName(databaseName);

    const printKey = () => {
        encryptionKeyQrCodeUtils.printEncryptionKey(
            keyText,
            fileName,
            encryptionKeyQrCodeUtils.getQrCodeHtml(encryptionKey)
        );
    };

    return (
        <div className="vstack gap-3">
            <div className="vstack gap-2">
                <FormGroup marginClass="m-0">
                    <FormLabel>
                        {t("encryption.key")}{" "}
                        <small className="fw-semibold text-muted">{t("encryption.keyEncoding")}</small>
                    </FormLabel>
                    <div className="d-flex gap-1">
                        <InputGroup className="flex-grow-1 flex-nowrap">
                            <Form.Control
                                ref={ref}
                                type="text"
                                value={encryptionKey ?? ""}
                                onChange={(event: ChangeEvent<HTMLInputElement>) => {
                                    onChange(event.currentTarget.value);
                                    setValue("isEncryptionKeySaved", false, { shouldDirty: true });
                                }}
                                onBlur={onBlur}
                                isInvalid={invalid}
                                disabled={isDisabled}
                                aria-label={t("encryption.keyValueLabel")}
                                autoComplete="off"
                            />
                            <Button
                                variant="secondary"
                                title={t("encryption.generateKey")}
                                onClick={regenerate}
                                disabled={isDisabled || isGenerating}
                            >
                                {isGenerating ? <Spinner size="sm" /> : <Icon icon="reset" margin="m-0" />}
                            </Button>
                        </InputGroup>
                        <Button
                            variant="secondary"
                            title={t("encryption.downloadKey")}
                            disabled={!isKeyValid}
                            onClick={() => fileDownloader.downloadAsTxt(keyText, fileName)}
                        >
                            <Icon icon="download" margin="m-0" />
                        </Button>
                        <Dropdown as={ButtonGroup}>
                            <Button
                                variant="secondary"
                                disabled={!isKeyValid}
                                onClick={() => copyToClipboard.copy(keyText, t("encryption.keyCopied"))}
                            >
                                <Icon icon="copy" />
                                {commonT("copy")}
                            </Button>
                            <Dropdown.Toggle
                                split
                                variant="secondary"
                                disabled={!isKeyValid}
                                aria-label={t("encryption.moreKeyOptions")}
                            />
                            <Dropdown.Menu align="end">
                                <Dropdown.Item as="button" type="button" onClick={showQrCode}>
                                    <Icon icon="open-modal" />
                                    {t("encryption.showQrCode")}
                                </Dropdown.Item>
                                <Dropdown.Item as="button" type="button" onClick={printKey}>
                                    <Icon icon="print" />
                                    {t("encryption.print")}
                                </Dropdown.Item>
                            </Dropdown.Menu>
                        </Dropdown>
                    </div>
                    {error && <FormValidationMessage>{error.message}</FormValidationMessage>}
                </FormGroup>
                {isNewKey && <RichAlert variant="warning">{t("encryption.newKeyWarning")}</RichAlert>}
                {isOriginalKey && <div className="small text-muted">{t("encryption.existingKeyInfo")}</div>}
            </div>
            {isNewKey && (
                <FormCheckbox control={control} name="isEncryptionKeySaved" disabled={isDisabled}>
                    {t("encryption.keySavedCheckbox")}
                </FormCheckbox>
            )}
            {isQrCodeVisible && (
                <EditBackupTaskEncryptionKeyQrCodeModal encryptionKey={encryptionKey} onClose={hideQrCode} />
            )}
        </div>
    );
}
