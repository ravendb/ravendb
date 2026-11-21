import Button from "react-bootstrap/Button";
import { FormProvider, SubmitHandler, useForm, useFormContext, useWatch } from "react-hook-form";
import { yupResolver } from "@hookform/resolvers/yup";
import { Icon } from "components/common/Icon";
import { FormGroup, FormLabel } from "components/common/Form";
import ConnectionTestErrorModal from "components/common/connectionTests/ConnectionTestErrorModal";
import { ConditionalPopover } from "components/common/ConditionalPopover";
import RadioButtons from "components/common/toggles/RadioButtons";
import Select, {
    OptionWithIconAndLicenseBadge,
    SelectOptionWithIconAndLicense,
    SingleValueWithIcon,
} from "components/common/select/Select";
import { useSheetDirtyFlag, useViewSheet, ViewSheet } from "components/common/splitView/ViewSheet";
import { accessManagerSelectors } from "components/common/shell/accessManagerSliceSelectors";
import { databaseSelectors } from "components/common/shell/databaseSliceSelectors";
import { licenseSelectors } from "components/common/shell/licenseSlice";
import { useAppSelector } from "components/store";
import OverrideConfiguration from "./OverrideConfiguration";
import { LocalFields } from "./Local";
import { AmazonS3Fields } from "./AmazonS3";
import { AzureFields } from "./Azure";
import { GoogleCloudFields } from "./GoogleCloud";
import { AmazonGlacierFields } from "./AmazonGlacier";
import { FtpFields } from "./Ftp";
import DestinationTestButton from "./DestinationTestButton";
import { useDestinationTest } from "./useDestinationTest";
import { destinationsSchema } from "./utils/formDestinationsValidation";
import { DestinationType, FormDestinationData, FormDestinations } from "./utils/formDestinationsTypes";
import { formDestinationsUtils } from "./utils/formDestinationsUtils";
import { useStudioTranslation } from "hooks/useStudioTranslation";

type AuthenticationMethod = "static" | "script";

export interface DestinationEditorSheetProps {
    editedType: DestinationType | null;
    destinations: FormDestinations["destinations"];
    allowedDestinations?: backupOptions[];
    allowedAwsRegions?: string[];
    localRootPath?: string;
    isAwsSessionTokenVisible?: boolean;
    isS3StorageClassVisible?: boolean;
    onApply: (type: DestinationType, data: FormDestinationData, previousType: DestinationType | null) => void;
}

export default function DestinationEditorSheet(props: DestinationEditorSheetProps) {
    const form = useForm<FormDestinations>({
        mode: "all",
        resolver: yupResolver(destinationsSchema),
        defaultValues: {
            destinations: getEditorDestinations(props.destinations, props.editedType, props.isS3StorageClassVisible),
        },
    });

    return (
        <FormProvider {...form}>
            <DestinationEditorForm {...props} />
        </FormProvider>
    );
}

function DestinationEditorForm({
    editedType,
    destinations,
    allowedDestinations,
    allowedAwsRegions,
    localRootPath,
    isAwsSessionTokenVisible,
    isS3StorageClassVisible,
    onApply,
}: DestinationEditorSheetProps) {
    const { close, forceClose } = useViewSheet();
    const t = useStudioTranslation("formDestinations");
    const commonT = useStudioTranslation("common");
    const hasCloudBackups = useAppSelector(licenseSelectors.statusValue("HasCloudBackups"));
    const isClusterAdminOrClusterNode = useAppSelector(accessManagerSelectors.isClusterAdminOrClusterNode);
    const isExternalScriptRestricted = useAppSelector(
        databaseSelectors.isRestrictExternalScriptUsageForNonClusterAdmin
    );

    const { control, handleSubmit, setValue, formState } = useFormContext<FormDestinations>();
    useSheetDirtyFlag(formState.isDirty);

    const formDestinations = useWatch({ control, name: "destinations" });
    const selectedType = formDestinationsUtils.getEnabledDestinationTypes(formDestinations)[0] ?? null;
    const isOverrideConfig = selectedType ? !!formDestinations[selectedType].config?.isOverrideConfig : false;
    const isTestAvailable =
        !!selectedType &&
        formDestinationsUtils.isCredentialsTestAvailable(selectedType, formDestinations[selectedType]);
    const isScriptDisabled = !isClusterAdminOrClusterNode && isExternalScriptRestricted && !isOverrideConfig;

    const asyncTest = useDestinationTest(selectedType ?? "local");

    const usedTypes = formDestinationsUtils.getAddedDestinationTypes(destinations).filter((x) => x !== editedType);
    const typeOptions: SelectOptionWithIconAndLicense<DestinationType>[] =
        formDestinationsUtils.DESTINATION_TYPES.filter((type) => !usedTypes.includes(type)).map((type) => {
            const info = formDestinationsUtils.DESTINATION_TYPE_INFO[type];
            const typeLabel = t(`types.${type}`);
            const isAllowed = formDestinationsUtils.isDestinationAllowed(type, allowedDestinations);
            const isLicensed = formDestinationsUtils.isDestinationLicensed(type, hasCloudBackups);

            return {
                value: type,
                label: isAllowed ? typeLabel : t("editor.disabledByAdmin", { destination: typeLabel }),
                icon: info.icon,
                isDisabled: !isAllowed || !isLicensed,
                licenseRequired: isLicensed ? undefined : formDestinationsUtils.CLOUD_DESTINATIONS_LICENSE,
            };
        });

    const selectType = (type: DestinationType) => {
        const initialDestinations = getEditorDestinations(destinations, editedType, isS3StorageClassVisible);
        formDestinationsUtils.DESTINATION_TYPES.forEach((x) => {
            setValue(`destinations.${x}`, { ...initialDestinations[x], isEnabled: x === type }, { shouldDirty: true });
        });
    };

    const setAuthenticationMethod = (method: AuthenticationMethod) => {
        setValue(`destinations.${selectedType}.config.isOverrideConfig`, method === "script", { shouldDirty: true });
    };

    const handleApply: SubmitHandler<FormDestinations> = (data) => {
        const isEditedDestinationDisabled = selectedType === editedType && !destinations[editedType].isEnabled;
        onApply(
            selectedType,
            { ...data.destinations[selectedType], isEnabled: !isEditedDestinationDisabled },
            editedType
        );
        forceClose();
    };

    return (
        <form className="h-100 d-flex flex-column" onSubmit={handleSubmit(handleApply)} autoComplete="off">
            <ViewSheet.Header className="panel-bg-2 align-items-center" isPinHidden>
                <h3 className="m-0">
                    <Icon icon="global" addon="settings" color="primary" margin="me-1" />
                    {editedType ? t("editor.editTitle") : t("editor.newTitle")}
                </h3>
            </ViewSheet.Header>
            <ViewSheet.Body className="w-100 flex-grow-1 vstack gap-3 p-4 overflow-auto">
                <FormGroup marginClass="m-0">
                    <FormLabel>{t("editor.destination")}</FormLabel>
                    <Select
                        aria-label={t("editor.destination")}
                        placeholder={t("editor.destinationPlaceholder")}
                        options={typeOptions}
                        value={typeOptions.find((x) => x.value === selectedType) ?? null}
                        onChange={(option: SelectOptionWithIconAndLicense<DestinationType>) => selectType(option.value)}
                        components={{ Option: OptionWithIconAndLicenseBadge, SingleValue: SingleValueWithIcon }}
                    />
                </FormGroup>
                {selectedType && (
                    <>
                        <FormGroup marginClass="m-0">
                            <FormLabel>{t("authenticationMethod")}</FormLabel>
                            <ConditionalPopover
                                className="w-100"
                                conditions={{
                                    isActive: isScriptDisabled,
                                    message: t("scriptNotAllowed"),
                                }}
                            >
                                <RadioButtons<AuthenticationMethod>
                                    className="flex-grow-1"
                                    aria-label={t("authenticationMethod")}
                                    items={[
                                        {
                                            value: "static",
                                            label: t("staticCredentials"),
                                            helpMessage: t("staticCredentialsHelp"),
                                        },
                                        {
                                            value: "script",
                                            label: t("externalScript"),
                                            helpMessage: t("externalScriptHelp"),
                                        },
                                    ]}
                                    selected={isOverrideConfig ? "script" : "static"}
                                    onSelect={setAuthenticationMethod}
                                    isDisabled={isScriptDisabled}
                                />
                            </ConditionalPopover>
                        </FormGroup>
                        {isOverrideConfig ? (
                            <OverrideConfiguration fieldBase={`destinations.${selectedType}`} />
                        ) : (
                            <StaticCredentialsFields
                                key={selectedType}
                                type={selectedType}
                                isNew={!editedType}
                                allowedAwsRegions={allowedAwsRegions}
                                localRootPath={localRootPath}
                                isAwsSessionTokenVisible={isAwsSessionTokenVisible}
                                isS3StorageClassVisible={isS3StorageClassVisible}
                            />
                        )}
                        {asyncTest.isErrorModalOpen && (
                            <ConnectionTestErrorModal
                                error={asyncTest.result.Error}
                                onClose={asyncTest.closeErrorModal}
                            />
                        )}
                    </>
                )}
            </ViewSheet.Body>
            <ViewSheet.Footer className="w-100 p-2 panel-bg-2 border-top d-flex justify-content-between border-secondary">
                <Button onClick={close} variant="link" className="text-muted">
                    {commonT("cancel")}
                </Button>
                <div className="hstack gap-2">
                    {isTestAvailable && <DestinationTestButton asyncTest={asyncTest} className="rounded-pill" />}
                    <Button className="rounded-pill" type="submit" disabled={!selectedType}>
                        <Icon icon="save" />
                        {t("editor.apply")}
                    </Button>
                </div>
            </ViewSheet.Footer>
        </form>
    );
}

interface StaticCredentialsFieldsProps {
    type: DestinationType;
    isNew: boolean;
    allowedAwsRegions?: string[];
    localRootPath?: string;
    isAwsSessionTokenVisible?: boolean;
    isS3StorageClassVisible?: boolean;
}

function StaticCredentialsFields({
    type,
    isNew,
    allowedAwsRegions,
    localRootPath,
    isAwsSessionTokenVisible,
    isS3StorageClassVisible,
}: StaticCredentialsFieldsProps) {
    const t = useStudioTranslation("formDestinations");

    switch (type) {
        case "local":
            return (
                <LocalFields
                    rootPath={localRootPath}
                    label={t("local.backupDirectory")}
                    selectorTitle={t("local.selectBackupDirectory")}
                />
            );
        case "s3":
            return (
                <AmazonS3Fields
                    allowedRegions={allowedAwsRegions}
                    isSessionTokenVisible={isAwsSessionTokenVisible}
                    isStorageClassVisible={isS3StorageClassVisible}
                />
            );
        case "azure":
            return <AzureFields />;
        case "googleCloud":
            return <GoogleCloudFields isCredentialsVisibleByDefault={isNew} />;
        case "glacier":
            return (
                <AmazonGlacierFields
                    allowedRegions={allowedAwsRegions}
                    isSessionTokenVisible={isAwsSessionTokenVisible}
                />
            );
        case "ftp":
            return <FtpFields />;
    }
}

function getEditorDestinations(
    destinations: FormDestinations["destinations"],
    editedType: DestinationType | null,
    isS3StorageClassVisible: boolean
): FormDestinations["destinations"] {
    const s3 = getEditorDestination("s3", destinations, editedType);

    return {
        local: getEditorDestination("local", destinations, editedType),
        s3: isS3StorageClassVisible
            ? { ...s3, storageClass: s3.storageClass ?? formDestinationsUtils.DEFAULT_S3_STORAGE_CLASS }
            : s3,
        azure: getEditorDestination("azure", destinations, editedType),
        googleCloud: getEditorDestination("googleCloud", destinations, editedType),
        glacier: getEditorDestination("glacier", destinations, editedType),
        ftp: getEditorDestination("ftp", destinations, editedType),
    };
}

function getEditorDestination<T extends DestinationType>(
    type: T,
    destinations: FormDestinations["destinations"],
    editedType: DestinationType | null
): FormDestinations["destinations"][T] {
    if (type === editedType) {
        return { ...destinations[type], isEnabled: true };
    }
    return { ...formDestinationsUtils.DESTINATION_TYPE_INFO[type].defaultData, isEnabled: false };
}
