import React, { useMemo, useState } from "react";
import { FieldErrors, FormProvider, useForm, useFormContext, useWatch } from "react-hook-form";
import Alert from "react-bootstrap/Alert";
import Button from "react-bootstrap/Button";
import classNames from "classnames";
import { Icon } from "components/common/Icon";
import FeatureNotAvailable from "components/common/FeatureNotAvailable";
import { AccessPopover } from "components/common/AccessPopover";
import { useAppUrls } from "components/hooks/useAppUrls";
import { useAppSelector } from "components/store";
import { databaseSelectors } from "components/common/shell/databaseSliceSelectors";
import { accessManagerSelectors } from "components/common/shell/accessManagerSliceSelectors";
import ImportPageLayout from "components/pages/database/tasks/importData/shared/ImportPageLayout";
import ImportSection from "components/pages/database/tasks/importData/shared/ImportSection";
import DocumentsToImportCard from "components/pages/database/tasks/importData/shared/DocumentsToImportCard";
import ConfigurationToImportSection from "components/pages/database/tasks/importData/shared/ConfigurationToImportSection";
import TransformScriptCard from "components/pages/database/tasks/importData/shared/TransformScriptCard";
import { useImportPageScroll } from "components/pages/database/tasks/importData/shared/useImportPageScroll";
import {
    getFirstErrorPath,
    getSectionIdForErrorPath,
} from "components/pages/database/tasks/importData/shared/importNav";
import {
    ImportRestrictionsProvider,
    useImportRestrictions,
} from "components/pages/database/tasks/importData/shared/useImportRestrictions";
import {
    useImportOptionsFormSync,
    withRestrictedDefaults,
} from "components/pages/database/tasks/importData/shared/useImportOptionsFormSync";
import {
    MigrateRavenDbFormData,
    MigrateRavenDbValidationContext,
    migrateRavenDbYupResolver,
} from "./migrateRavenDbValidation";
import { getDefaultMigrateFormData, hasAnyMigrateInclude } from "./migrateRavenDbUtils";
import {
    getEffectiveAuthenticationMethod,
    getUnsupportedVersionReason,
    getVersionToggleRestrictions,
    getVisibleDocumentToggles,
    isLegacyVersion,
} from "./migrateVersionSupport";
import { RemoteServerInfo, useRemoteServerInfo } from "./useRemoteServerInfo";
import { useMigrateOperation, useMigrateRestrictedKeys } from "./useMigrateOperation";
import { ERROR_FIELD_TARGETS, SECTION_NAV, SELECT_SERVER_SECTION } from "./migrateRavenDbNav";
import SelectServerSection from "./SelectServerSection";

export default function MigrateRavenDbDatabase() {
    const isSharded = useAppSelector(databaseSelectors.activeDatabase)?.isSharded;

    if (isSharded) {
        return (
            <FeatureNotAvailable>
                <span>
                    Migrate data from another RavenDB Server is not available for{" "}
                    <Icon icon="sharding" color="shard" margin="m-0" /> sharded databases
                </span>
            </FeatureNotAvailable>
        );
    }

    return (
        <ImportRestrictionsProvider>
            <MigrateRavenDbForm />
        </ImportRestrictionsProvider>
    );
}

function MigrateRavenDbForm() {
    const isAdminAccessOrAbove = useAppSelector(accessManagerSelectors.getHasDatabaseAdminAccess)();
    const canDetect = useAppSelector(accessManagerSelectors.isOperatorOrAbove);
    const restrictions = useImportRestrictions();
    const [validationContext, setValidationContext] = useState<MigrateRavenDbValidationContext>({
        isRavenFsImport: false,
        effectiveAuthenticationMethod: "none",
    });

    const form = useForm<MigrateRavenDbFormData, MigrateRavenDbValidationContext>({
        resolver: migrateRavenDbYupResolver,
        mode: "onChange",
        context: validationContext,
        defaultValues: withRestrictedDefaults(getDefaultMigrateFormData(isAdminAccessOrAbove), restrictions),
    });
    const { control } = form;

    const serverInfo = useRemoteServerInfo(control, canDetect);
    const { version } = serverInfo;
    const isLegacy = !!version && isLegacyVersion(version);
    const [isImportRavenFs, authenticationMethod] = useWatch({
        control,
        name: ["server.isImportRavenFs", "server.authenticationMethod"],
    });
    const isRavenFsImport = isLegacy && isImportRavenFs;
    const effectiveAuthenticationMethod = getEffectiveAuthenticationMethod(authenticationMethod, version);
    // the context depends on the detected version, which needs the form's control, so it is stored after useForm
    if (
        validationContext.isRavenFsImport !== isRavenFsImport ||
        validationContext.effectiveAuthenticationMethod !== effectiveAuthenticationMethod
    ) {
        setValidationContext({ isRavenFsImport, effectiveAuthenticationMethod });
    }

    const versionRestrictions = useMemo(() => getVersionToggleRestrictions(version), [version]);

    return (
        <FormProvider {...form}>
            <ImportRestrictionsProvider documentToggleOverrides={versionRestrictions}>
                <MigrateRavenDbContent
                    serverInfo={serverInfo}
                    canDetect={canDetect}
                    isRavenFsImport={isRavenFsImport}
                />
            </ImportRestrictionsProvider>
        </FormProvider>
    );
}

interface MigrateRavenDbContentProps {
    serverInfo: RemoteServerInfo;
    canDetect: boolean;
    isRavenFsImport: boolean;
}

function MigrateRavenDbContent({ serverInfo, canDetect, isRavenFsImport }: MigrateRavenDbContentProps) {
    const { forCurrentDatabase } = useAppUrls();
    const form = useFormContext<MigrateRavenDbFormData>();
    const { control, handleSubmit, formState } = form;
    const restrictedKeys = useMigrateRestrictedKeys();
    const { migrate } = useMigrateOperation();

    useImportOptionsFormSync(form, getDefaultMigrateFormData);

    const { version, error, isAuthorized } = serverInfo;
    const [server, documents, configuration] = useWatch({ control, name: ["server", "documents", "configuration"] });
    const isLegacy = !!version && isLegacyVersion(version);
    const navItems = isRavenFsImport ? [SELECT_SERVER_SECTION] : SECTION_NAV;
    const scroll = useImportPageScroll(navItems);

    const hasInclude = !version || hasAnyMigrateInclude({ server, documents, configuration }, version, restrictedKeys);
    const isUnauthorizedLegacy = isLegacy && !isAuthorized;
    const canMigrate = !!version && !error && !isUnauthorizedLegacy && hasInclude && !formState.isSubmitting;

    const onInvalidSubmit = (errors: FieldErrors<MigrateRavenDbFormData>) => {
        const sectionId = getSectionIdForErrorPath(getFirstErrorPath(errors) ?? "", ERROR_FIELD_TARGETS);
        if (sectionId) {
            scroll.selectSection(sectionId);
        }
    };

    return (
        <ImportPageLayout
            title="Migrate data from another RavenDB Server"
            icon="import-database"
            backUrl={forCurrentDatabase.importDataOptionsUrl()}
            navItems={navItems}
            scroll={scroll}
            alert={
                <Alert variant="info" className="w-50">
                    <Icon icon="info" color="info" /> Note: Migration will overwrite any existing documents and indexes.
                </Alert>
            }
            actions={
                <AccessPopover accessRequired="DatabaseAdmin">
                    <Button
                        variant="primary"
                        className="rounded-pill"
                        disabled={!canMigrate}
                        onClick={handleSubmit((data) => migrate(data, version), onInvalidSubmit)}
                    >
                        <Icon icon="import-database" /> Migrate database
                    </Button>
                </AccessPopover>
            }
            banner={
                !hasInclude && (
                    <Alert variant="warning" className="mb-2">
                        Note: At least one &apos;include&apos; option must be checked.
                    </Alert>
                )
            }
        >
            <SelectServerSection serverInfo={serverInfo} canDetect={canDetect} />
            {!isRavenFsImport && (
                <fieldset disabled={!version} className={classNames({ "item-disabled": !version })}>
                    <ImportSection<MigrateRavenDbFormData> id="data-to-import" title="Data to import">
                        <DocumentsToImportCard visibleToggles={getVisibleDocumentToggles(version)} />
                    </ImportSection>
                    <ConfigurationToImportSection
                        unsupportedReason={isLegacy ? getUnsupportedVersionReason(version) : undefined}
                    />
                    <ImportSection<MigrateRavenDbFormData>
                        id="import-processing"
                        title="Import processing"
                        errorPaths={["processing.transformScript"]}
                    >
                        <TransformScriptCard />
                    </ImportSection>
                </fieldset>
            )}
        </ImportPageLayout>
    );
}
