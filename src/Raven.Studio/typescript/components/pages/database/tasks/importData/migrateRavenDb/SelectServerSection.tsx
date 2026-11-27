import React from "react";
import { useFormContext, useWatch } from "react-hook-form";
import { useAsync } from "react-async-hook";
import Card from "react-bootstrap/Card";
import Form from "react-bootstrap/Form";
import {
    FormGroup,
    FormInput,
    FormLabel,
    FormRadioToggleWithIcon,
    FormSelectAutocomplete,
    FormSwitch,
} from "components/common/Form";
import RichAlert from "components/common/RichAlert";
import { LoadingDots } from "components/common/LoadingDots";
import SegmentedControl, { SegmentedControlItem } from "components/common/SegmentedControl";
import { SelectOption } from "components/common/select/Select";
import { RadioToggleWithIconInputItem } from "components/common/toggles/RadioToggle";
import { useAppSelector } from "components/store";
import { databaseSelectors } from "components/common/shell/databaseSliceSelectors";
import { useServices } from "components/hooks/useServices";
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
    const resourceLabel = isRavenFsImport ? "File system name" : "Database name";

    return (
        <ImportSection<MigrateRavenDbFormData>
            id={SELECT_SERVER_SECTION.id}
            title={SELECT_SERVER_SECTION.label}
            errorPaths={["server"]}
        >
            <Card className="p-4 mb-4">
                {!canDetect && (
                    <RichAlert variant="info" className="mb-3">
                        Migrating requires Operator security clearance (needed to detect the source server version).
                    </RichAlert>
                )}
                <FormGroup>
                    <FormLabel>Server URL</FormLabel>
                    <FormSelectAutocomplete
                        control={control}
                        name="server.serverUrl"
                        aria-label="Server URL"
                        placeholder="e.g. https://a.ravendb.example:8080"
                        options={toOptions(asyncServerUrls.result?.List)}
                        isLoading={asyncServerUrls.loading}
                    />
                </FormGroup>
                {lowerCaseUrl.startsWith("https://") && (
                    <FormGroup>
                        <FormSwitch control={control} name="server.isSkipServerCertificateValidation">
                            Skip Server Certificate Validation
                        </FormSwitch>
                    </FormGroup>
                )}
                <FormGroup>
                    <FormLabel>Server version</FormLabel>
                    <div className="position-relative">
                        <Form.Control readOnly value={isLoading ? "" : formatServerVersion(version)} />
                        {isLoading && (
                            <LoadingDots
                                label="Detecting server version"
                                className="position-absolute top-50 end-0 translate-middle-y"
                            />
                        )}
                    </div>
                    {error && !isLoading && <div className="text-danger mt-1 text-break">{error}</div>}
                </FormGroup>
                {isLegacy && (
                    <FormGroup>
                        <FormLabel>Resource type</FormLabel>
                        <FormRadioToggleWithIcon
                            control={control}
                            name="server.isImportRavenFs"
                            leftItem={DATABASE_ITEM}
                            rightItem={RAVEN_FS_ITEM}
                        />
                    </FormGroup>
                )}
                {isLegacy && (
                    <>
                        <FormGroup>
                            <FormLabel>Authentication method</FormLabel>
                            <SegmentedControl<AuthenticationMethod>
                                items={AUTHENTICATION_ITEMS}
                                selected={authenticationMethod}
                                onSelect={(method) =>
                                    setValue("server.authenticationMethod", method, { shouldValidate: true })
                                }
                            />
                        </FormGroup>
                        {authenticationMethod === "windows" && (
                            <>
                                <FormGroup>
                                    <FormLabel>User name</FormLabel>
                                    <FormInput
                                        control={control}
                                        name="server.userName"
                                        type="text"
                                        placeholder="e.g. john.doe"
                                    />
                                </FormGroup>
                                <FormGroup>
                                    <FormLabel>Password</FormLabel>
                                    <FormInput
                                        control={control}
                                        name="server.password"
                                        type="password"
                                        autoComplete="off"
                                    />
                                </FormGroup>
                                <FormGroup>
                                    <FormLabel>Domain</FormLabel>
                                    <FormInput
                                        control={control}
                                        name="server.domain"
                                        type="text"
                                        placeholder="e.g. CORP"
                                    />
                                </FormGroup>
                            </>
                        )}
                        {authenticationMethod === "apiKey" && (
                            <>
                                <FormGroup>
                                    <FormLabel>API Key</FormLabel>
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
                                            Enable Basic Authentication Over Unsecured Http
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
                        placeholder={isRavenFsImport ? "e.g. FileSystem1" : "e.g. Northwind"}
                        options={toOptions(isRavenFsImport ? fileSystemNames : databaseNames)}
                        isLoading={isLoading}
                    />
                    {isLegacy && !isAuthorized && (
                        <div className="text-warning mt-1">
                            Unauthorized access to the server, please enter your credentials
                        </div>
                    )}
                </FormGroup>
            </Card>
        </ImportSection>
    );
}

function toOptions(values: string[] = []): SelectOption[] {
    return values.map((value) => ({ label: value, value }));
}

const DATABASE_ITEM: RadioToggleWithIconInputItem<boolean> = {
    label: "Database",
    value: false,
    iconName: "database",
};

const RAVEN_FS_ITEM: RadioToggleWithIconInputItem<boolean> = {
    label: "RavenFS",
    value: true,
    iconName: "filesystem",
};

const AUTHENTICATION_ITEMS: SegmentedControlItem<AuthenticationMethod>[] = [
    { value: "none", label: "None" },
    { value: "apiKey", label: "API Key" },
    { value: "windows", label: "Windows Credentials" },
];
