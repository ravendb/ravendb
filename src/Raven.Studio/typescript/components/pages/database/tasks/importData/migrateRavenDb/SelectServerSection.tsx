import React from "react";
import { useFormContext, useWatch } from "react-hook-form";
import { useAsync } from "react-async-hook";
import Card from "react-bootstrap/Card";
import Form from "react-bootstrap/Form";
import Spinner from "react-bootstrap/Spinner";
import {
    FormGroup,
    FormInput,
    FormLabel,
    FormRadioToggleWithIcon,
    FormSelectAutocomplete,
    FormSwitch,
} from "components/common/Form";
import RichAlert from "components/common/RichAlert";
import SegmentedControl from "components/common/SegmentedControl";
import { SelectOption } from "components/common/select/Select";
import { useAppSelector } from "components/store";
import { databaseSelectors } from "components/common/shell/databaseSliceSelectors";
import { useServices } from "components/hooks/useServices";
import { useStudioTranslation } from "components/hooks/useStudioTranslation";
import ImportSection from "components/pages/database/tasks/importData/shared/ImportSection";
import {
    AuthenticationMethod,
    MigrateRavenDbFormData,
} from "components/pages/database/tasks/importData/migrateRavenDb/migrateRavenDbValidation";
import {
    formatServerVersion,
    isLegacyVersion,
} from "components/pages/database/tasks/importData/migrateRavenDb/migrateVersionSupport";
import { RemoteServerInfo } from "components/pages/database/tasks/importData/migrateRavenDb/useRemoteServerInfo";
import { SELECT_SERVER_SECTION } from "components/pages/database/tasks/importData/migrateRavenDb/migrateRavenDbNav";

interface SelectServerSectionProps {
    serverInfo: RemoteServerInfo;
    canDetect: boolean;
}

export default function SelectServerSection({ serverInfo, canDetect }: SelectServerSectionProps) {
    const t = useStudioTranslation("importData");
    const { control, setValue } = useFormContext<MigrateRavenDbFormData>();
    const databaseName = useAppSelector(databaseSelectors.activeDatabaseName);
    const { tasksService } = useServices();
    const [serverUrl, authenticationMethod, isImportRavenFs] = useWatch({
        control,
        name: ["server.serverUrl", "server.authenticationMethod", "server.isImportRavenFs"],
    });

    const asyncServerUrls = useAsync(() => tasksService.getMigratedServerUrls(databaseName), [databaseName]);

    const { version, databaseNames, fileSystemNames, isAuthorized, hasUnsecuredBasicAuthOption, error, isLoading } =
        serverInfo;
    const isLegacy = !!version && isLegacyVersion(version);
    const isRavenFsImport = isLegacy && isImportRavenFs;
    const lowerCaseUrl = serverUrl.toLowerCase();
    const resourceContext = isRavenFsImport ? "fileSystem" : "database";
    const resourceLabel = t("migrate.resourceName", { context: resourceContext });

    return (
        <ImportSection<MigrateRavenDbFormData>
            id={SELECT_SERVER_SECTION.id}
            title={t(SELECT_SERVER_SECTION.label)}
            errorPaths={["server"]}
        >
            <Card className="p-4 mb-4">
                {!canDetect && (
                    <RichAlert variant="info" className="mb-3">
                        {t("migrate.operatorRequired")}
                    </RichAlert>
                )}
                <FormGroup>
                    <FormLabel>{t("migrate.serverUrl")}</FormLabel>
                    <FormSelectAutocomplete
                        control={control}
                        name="server.serverUrl"
                        aria-label={t("migrate.serverUrl")}
                        placeholder={t("migrate.serverUrlPlaceholder")}
                        options={toOptions(asyncServerUrls.result?.List)}
                        isLoading={asyncServerUrls.loading}
                    />
                </FormGroup>
                {lowerCaseUrl.startsWith("https://") && (
                    <FormGroup>
                        <FormSwitch control={control} name="server.isSkipServerCertificateValidation">
                            {t("migrate.skipCertificateValidation")}
                        </FormSwitch>
                    </FormGroup>
                )}
                <FormGroup>
                    <FormLabel>{t("migrate.serverVersion")}</FormLabel>
                    <div className="position-relative">
                        <Form.Control readOnly value={isLoading ? "" : formatServerVersion(version)} />
                        {isLoading && (
                            <span className="position-absolute top-50 end-0 translate-middle-y me-2 d-flex">
                                <Spinner size="sm" role="status" aria-label={t("migrate.detectingVersion")} />
                            </span>
                        )}
                    </div>
                    {error && !isLoading && <div className="text-danger mt-1 text-break">{error}</div>}
                </FormGroup>
                {isLegacy && (
                    <FormGroup>
                        <FormLabel>{t("migrate.resourceType")}</FormLabel>
                        <FormRadioToggleWithIcon
                            control={control}
                            name="server.isImportRavenFs"
                            leftItem={{ label: t("migrate.resourceTypeDatabase"), value: false, iconName: "database" }}
                            rightItem={{ label: t("migrate.resourceTypeRavenFs"), value: true, iconName: "filesystem" }}
                        />
                    </FormGroup>
                )}
                {isLegacy && (
                    <>
                        <FormGroup>
                            <FormLabel>{t("migrate.authenticationMethod")}</FormLabel>
                            <SegmentedControl<AuthenticationMethod>
                                items={AUTHENTICATION_METHODS.map((value) => ({
                                    value,
                                    label: t(`migrate.authentication.${value}`),
                                }))}
                                selected={authenticationMethod}
                                onSelect={(method) =>
                                    setValue("server.authenticationMethod", method, { shouldValidate: true })
                                }
                            />
                        </FormGroup>
                        {authenticationMethod === "windows" && (
                            <>
                                <FormGroup>
                                    <FormLabel>{t("migrate.userName")}</FormLabel>
                                    <FormInput
                                        control={control}
                                        name="server.userName"
                                        type="text"
                                        placeholder={t("migrate.userNamePlaceholder")}
                                    />
                                </FormGroup>
                                <FormGroup>
                                    <FormLabel>{t("migrate.password")}</FormLabel>
                                    <FormInput
                                        control={control}
                                        name="server.password"
                                        type="password"
                                        autoComplete="off"
                                    />
                                </FormGroup>
                                <FormGroup>
                                    <FormLabel>{t("migrate.domain")}</FormLabel>
                                    <FormInput
                                        control={control}
                                        name="server.domain"
                                        type="text"
                                        placeholder={t("migrate.domainPlaceholder")}
                                    />
                                </FormGroup>
                            </>
                        )}
                        {authenticationMethod === "apiKey" && (
                            <>
                                <FormGroup>
                                    <FormLabel>{t("migrate.apiKey")}</FormLabel>
                                    <FormInput
                                        control={control}
                                        name="server.apiKey"
                                        type="password"
                                        autoComplete="off"
                                    />
                                </FormGroup>
                                {hasUnsecuredBasicAuthOption && lowerCaseUrl.startsWith("http://") && (
                                    <FormGroup>
                                        <FormSwitch
                                            control={control}
                                            name="server.isEnableBasicAuthenticationOverUnsecuredHttp"
                                        >
                                            {t("migrate.enableBasicAuthentication")}
                                        </FormSwitch>
                                    </FormGroup>
                                )}
                            </>
                        )}
                    </>
                )}
                <FormGroup marginClass="mb-0">
                    <FormLabel>{resourceLabel}</FormLabel>
                    <FormSelectAutocomplete
                        control={control}
                        name="server.databaseName"
                        aria-label={resourceLabel}
                        placeholder={t("migrate.resourceNamePlaceholder", { context: resourceContext })}
                        options={toOptions(isRavenFsImport ? fileSystemNames : databaseNames)}
                        isLoading={isLoading}
                    />
                    {isLegacy && !isAuthorized && <div className="text-warning mt-1">{t("migrate.unauthorized")}</div>}
                </FormGroup>
            </Card>
        </ImportSection>
    );
}

function toOptions(values: string[] = []): SelectOption[] {
    return values.map((value) => ({ label: value, value }));
}

const AUTHENTICATION_METHODS: AuthenticationMethod[] = ["none", "apiKey", "windows"];
