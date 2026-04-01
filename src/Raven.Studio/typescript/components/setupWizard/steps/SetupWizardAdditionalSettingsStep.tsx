import { Control, useFormContext, useWatch } from "react-hook-form";
import { SetupWizardFormData } from "../setupWizardValidation";
import { Icon } from "components/common/Icon";
import Button from "react-bootstrap/Button";
import { FormGroup, FormInput, FormLabel, FormPathSelector, FormSelect, FormSwitch } from "components/common/Form";
import { RichAlert } from "components/common/RichAlert";
import { setupWizardConstants, setupWizardGA4Prefixes } from "components/setupWizard/utils/setupWizardConstants";
import { HrHeader } from "components/common/HrHeader";
import classNames from "classnames";
import { useEffect, useMemo } from "react";
import { getFullDomain, getLicenseType, sanitizeCommonName } from "components/setupWizard/utils/setupWizardUtils";
import PopoverWithHoverWrapper from "components/common/PopoverWithHoverWrapper";
import { ConditionalPopover } from "components/common/ConditionalPopover";
import { useServices } from "hooks/useServices";
import { useEventsCollector } from "components/hooks/useEventsCollector";
import { OperatingSystem, useOS } from "hooks/useOS";
import { SetupWizardInfoPopover } from "components/setupWizard/partials/SetupWizardInfoPopover";
import { setupWizardFormDefaultValues } from "components/setupWizard/utils/setupWizardFormDefaultValues";

export function SetupWizardAdditionalSettingsStep() {
    const { control } = useFormContext<SetupWizardFormData>();

    const {
        additionalSettingsStep,
        licenseKeyStep,
        securityStep: { securityOption },
    } = useWatch({ control });

    const { licenseInfo } = licenseKeyStep;

    useAdditionalSettingsFormSideEffects();

    return (
        <div className="setup-wizard-additional-settings">
            <div className="mb-4">
                <h2 className="mb-1">Additional settings</h2>
                <p className="mb-4 text-muted">At this step, you can configure optional settings for your setup.</p>
            </div>
            {getLicenseType(licenseInfo).isHigherThan("None") && securityOption !== "none" && (
                <div>
                    <ServerEnvironmentSection control={control} licenseInfo={licenseInfo} />
                    <CertificateExpirationSection control={control} />
                    <HrHeader />
                </div>
            )}
            <ExperimentalFeaturesSection control={control} licenseInfo={licenseInfo} />
            <div className="d-flex gap-2 align-items-center">
                <h3 className="mb-1">Configure advanced options</h3>
                <FormSwitch size="lg" name="additionalSettingsStep.isAdvancedSettingsVisible" control={control} />
            </div>
            <p className="text-muted">Settings you may want to configure if you&#39;re an experienced user.</p>
            <AdvancedSettingsContent control={control} isVisible={additionalSettingsStep.isAdvancedSettingsVisible} />
        </div>
    );
}

function useAdditionalSettingsFormSideEffects() {
    const { reportEvent } = useEventsCollector();
    const { setValue, watch, control } = useFormContext<SetupWizardFormData>();

    const { licenseKeyStep } = useWatch({ control });

    useEffect(() => {
        if (getLicenseType(licenseKeyStep.licenseInfo).isDeveloper()) {
            setValue("additionalSettingsStep.studioEnvironment", "Development", {
                shouldDirty: true,
            });
        } else if (getLicenseType(licenseKeyStep.licenseInfo).isProfessionalOrHigher()) {
            setValue("additionalSettingsStep.studioEnvironment", "Production", {
                shouldDirty: true,
            });
        }

        const { unsubscribe } = watch((values, { name }) => {
            if (
                name === "additionalSettingsStep.isAdvancedSettingsVisible" &&
                !values.additionalSettingsStep.isAdvancedSettingsVisible
            ) {
                setValue("additionalSettingsStep.dataDirectory", null);
                setValue("additionalSettingsStep.setupCertificatePath", null);
            }

            switch (name) {
                case "additionalSettingsStep.isAdvancedSettingsVisible": {
                    const state = values.additionalSettingsStep.isAdvancedSettingsVisible ? "enabled" : "disabled";
                    reportEvent(setupWizardGA4Prefixes.additionalSettingsStep, "toggle-advanced", state);
                    break;
                }
                case "additionalSettingsStep.studioEnvironment": {
                    const env = values.additionalSettingsStep.studioEnvironment;
                    if (env) {
                        reportEvent(setupWizardGA4Prefixes.additionalSettingsStep, "set-environment", env);
                    }
                    break;
                }
                case "additionalSettingsStep.adminCertificateExpirationTime": {
                    const months = values.additionalSettingsStep.adminCertificateExpirationTime;
                    if (months != null) {
                        reportEvent(
                            setupWizardGA4Prefixes.additionalSettingsStep,
                            "set-admin-cert-expiration",
                            String(months)
                        );
                    }
                    break;
                }
                case "additionalSettingsStep.postgresqlIntegration": {
                    reportEvent(setupWizardGA4Prefixes.additionalSettingsStep, "set-postgresql");
                    break;
                }
                case "additionalSettingsStep.dataDirectory": {
                    reportEvent(setupWizardGA4Prefixes.additionalSettingsStep, "set-data-dir");
                    break;
                }
                case "additionalSettingsStep.setupCertificatePath": {
                    reportEvent(setupWizardGA4Prefixes.additionalSettingsStep, "set-setup-cert-path");
                    break;
                }
                case "additionalSettingsStep.logsPath": {
                    reportEvent(setupWizardGA4Prefixes.additionalSettingsStep, "set-logs-path");
                    break;
                }
                case "additionalSettingsStep.autoIndexingEngineType": {
                    reportEvent(
                        setupWizardGA4Prefixes.additionalSettingsStep,
                        "set-auto-indexing-engine",
                        values.additionalSettingsStep.autoIndexingEngineType
                    );
                    break;
                }
                case "additionalSettingsStep.staticIndexingEngineType": {
                    reportEvent(
                        setupWizardGA4Prefixes.additionalSettingsStep,
                        "set-static-indexing-engine",
                        values.additionalSettingsStep.staticIndexingEngineType
                    );
                    break;
                }
            }
        });

        return () => unsubscribe();
    }, [watch, setValue]); // eslint-disable-line react-hooks/exhaustive-deps
}

const studioEnvironmentImage = require("Content/img/setupWizard/studioEnvironment.png");

function ServerEnvironmentSection({
    control,
    licenseInfo,
}: {
    control: Control<SetupWizardFormData>;
    licenseInfo: SetupWizardFormData["licenseKeyStep"]["licenseInfo"];
}) {
    if (getLicenseType(licenseInfo).isHigherThan("Community")) {
        return (
            <FormGroup>
                <FormLabel className="d-flex">
                    <div>Studio environment</div>
                    <PopoverWithHoverWrapper
                        message={
                            <>
                                <img
                                    src={studioEnvironmentImage}
                                    alt="studio-environment-image"
                                    className="mb-2 w-100"
                                />
                                <span>
                                    The Studio environment allows you to add a visual identifier to the UI, making it
                                    easier to distinguish between multiple environments when working simultaneously.
                                </span>
                            </>
                        }
                        placement="right"
                    >
                        <Icon icon="info-new" />
                    </PopoverWithHoverWrapper>
                </FormLabel>
                <FormSelect
                    control={control}
                    name="additionalSettingsStep.studioEnvironment"
                    options={setupWizardConstants.allStudioEnvironmentOptions}
                />
            </FormGroup>
        );
    }

    return null;
}

function CertificateExpirationSection({ control }: { control: Control<SetupWizardFormData> }) {
    const {
        securityStep: { securityOption },
    } = useWatch({ control });

    if (securityOption === "none") {
        return null;
    }

    return (
        <FormGroup>
            <FormLabel className="d-flex">
                Admin client certificate expiration time
                <PopoverWithHoverWrapper
                    message={
                        <>
                            This defines how long the admin client certificate will be valid. By default, this value is
                            set to 60 months.
                        </>
                    }
                    placement="right"
                >
                    <Icon icon="info-new" />
                </PopoverWithHoverWrapper>
            </FormLabel>
            <FormInput
                name="additionalSettingsStep.adminCertificateExpirationTime"
                placeholder="default: 60"
                control={control}
                type="number"
                addon="months"
            />
        </FormGroup>
    );
}

interface ExperimentalFeaturesSectionProps {
    control: Control<SetupWizardFormData>;
    licenseInfo: SetupWizardFormData["licenseKeyStep"]["licenseInfo"];
}

function ExperimentalFeaturesSection({ control, licenseInfo }: ExperimentalFeaturesSectionProps) {
    if (!getLicenseType(licenseInfo).isHigherThan("Community")) {
        return null;
    }

    return (
        <div>
            <h4 className="mb-0">Experimental features</h4>
            <p className="text-muted">
                Some newly released features are considered experimental and are disabled by default. Toggle on to
                enable them.
            </p>
            <PostgreSqlIntegrationToggle control={control} />
        </div>
    );
}

function PostgreSqlIntegrationToggle({ control }: { control: Control<SetupWizardFormData> }) {
    return (
        <div className="postgresql-integration bg-faded-experimental">
            <FormSwitch name="additionalSettingsStep.postgresqlIntegration" color="experimental" control={control} />
            <div className="d-flex w-100 align-items-center justify-content-between">
                <div className="flex-grow-1 lh-1">
                    <strong className="postgresql-integration__title">
                        PostgreSQL integration
                        <PopoverWithHoverWrapper
                            message={
                                <SetupWizardInfoPopover
                                    description="Enabling this feature allows RavenDB to function as a PostgreSQL server. Requires a license that includes the PostgreSQL Protocol."
                                    ravenLinkHash="TRKC2W"
                                />
                            }
                            placement="right"
                        >
                            <Icon icon="info-new" />
                        </PopoverWithHoverWrapper>
                    </strong>
                    <small className="postgresql-integration__description">
                        RavenDB supports the PostgreSQL protocol, enabling external tools such as Power BI to access the
                        database.
                    </small>
                </div>
                <Icon className="postgresql-integration__icon" size="lg" icon="integrations" />
            </div>
        </div>
    );
}

interface AdvancedSettingsContentProps {
    control: Control<SetupWizardFormData>;
    isVisible: boolean;
}

const getDefaultPath = (os: OperatingSystem, type: "certificate" | "dataDirectory" | "logs") => {
    const isUnix = os === "Linux" || os === "MacOS";

    const paths = {
        certificate: isUnix ? "e.g. /etc/ravendb/security/server.pfx" : "e.g. C:\\RavenDB\\Certificate\\server.pfx",
        dataDirectory: isUnix ? "e.g. /var/lib/ravendb/data" : "e.g. C:\\RavenDB\\Data",
        logs: isUnix ? "e.g. /logs" : "e.g. C:\\RavenDB\\Logs",
    };

    return paths[type];
};

const isAbsolutePath = (path: string | null | undefined): boolean => {
    if (!path) {
        return false;
    }

    // Windows absolute path: starts with drive letter (e.g., C:\) or UNC path (\\)
    if (/^[A-Za-z]:\\/.test(path) || /^\\\\/.test(path)) {
        return true;
    }

    // Unix/Mac absolute path: starts with /
    return path.startsWith("/");
};

function AdvancedSettingsContent({ control, isVisible }: AdvancedSettingsContentProps) {
    const os = useOS();
    const { resourcesService } = useServices();

    const {
        securityStep: { securityOption },
        additionalSettingsStep: { dataDirectory, logsPath, setupCertificatePath },
        selfSignedCertificateStep,
        domainStep,
    } = useWatch({
        control,
    });

    const getLocalFolderPathsProvider = (path: string) => {
        return async () => {
            const dto = await resourcesService.getFolderPathOptions_ServerLocal(path, true);
            return dto?.List || [];
        };
    };

    const defaultCertificatePathFileName = useMemo(() => {
        switch (securityOption) {
            case "ownCertificate":
                return sanitizeCommonName(`cluster.server.certificate.${selfSignedCertificateStep.cns[0]}.pfx`); // For own certificates, we use the first Common Name (CN) as the main identifier
            case "letsEncrypt":
                return `cluster.server.certificate.${getFullDomain(domainStep)}.pfx`;
            default:
                return undefined;
        }
    }, []);

    return (
        <div
            className={classNames({
                "item-disabled": !isVisible,
            })}
        >
            <FormGroup>
                <FormLabel className="hstack">
                    <div>Data directory</div>
                    <ConditionalPopover
                        conditions={{
                            isActive: isVisible,
                            message: (
                                <SetupWizardInfoPopover
                                    description={
                                        <ul>
                                            <li>Defines the path to the RavenDB data directory.</li>
                                            <li>
                                                By default, data is stored in the <code>RavenData</code> folder under
                                                the extracted <code>Server</code> directory.
                                            </li>
                                        </ul>
                                    }
                                    ravenLinkHash="GUNB5P"
                                />
                            ),
                        }}
                        popoverPlacement="right"
                    >
                        <Icon icon="info-new" />
                    </ConditionalPopover>
                </FormLabel>
                <FormPathSelector
                    disabled={!isVisible}
                    name="additionalSettingsStep.dataDirectory"
                    control={control}
                    placeholder={getDefaultPath(os, "dataDirectory")}
                    getPathsProvider={(path: string) => getLocalFolderPathsProvider(path)}
                    getPathDependencies={(path: string) => [path]}
                />
                {isVisible && isAbsolutePath(dataDirectory) && (
                    <RichAlert variant="warning" className="mt-2">
                        This path will be used on all nodes. Make sure this directory exists on each node; otherwise,
                        the setup will fail.
                    </RichAlert>
                )}
            </FormGroup>
            {securityOption !== "none" && (
                <FormGroup>
                    <FormLabel className="vstack">
                        <div className="hstack">
                            <div>Setup certificate path</div>
                            <ConditionalPopover
                                conditions={{
                                    isActive: isVisible,
                                    message: (
                                        <SetupWizardInfoPopover
                                            description={
                                                <ul>
                                                    <li>
                                                        Specifies the <strong>full file path</strong> (including the{" "}
                                                        <code>.pfx</code> extension) where the server certificate will
                                                        be stored.
                                                    </li>
                                                    <li>
                                                        Make sure the directory exists and has the required write
                                                        permissions on <strong>every node</strong>.
                                                    </li>
                                                    <li>
                                                        By default, RavenDB stores your server certificate directly
                                                        under the extracted <code>Server</code> folder.
                                                    </li>
                                                </ul>
                                            }
                                            ravenLinkHash="OXZ53O"
                                        />
                                    ),
                                }}
                                popoverPlacement="right"
                            >
                                <Icon icon="info-new" />
                            </ConditionalPopover>
                        </div>
                    </FormLabel>
                    <FormPathSelector
                        disabled={!isVisible}
                        name="additionalSettingsStep.setupCertificatePath"
                        control={control}
                        defaultFileName={defaultCertificatePathFileName}
                        placeholder={getDefaultPath(os, "certificate")}
                        getPathsProvider={(path: string) => getLocalFolderPathsProvider(path)}
                        getPathDependencies={(path: string) => [path]}
                    />
                    {isVisible && !!setupCertificatePath && (
                        <RichAlert variant="warning" className="mt-2">
                            This path must exist as the certificate will be saved here. After setup, ensure the
                            certificate is available at the same path on every node; otherwise, cluster setup may fail.
                        </RichAlert>
                    )}
                </FormGroup>
            )}
            <FormGroup>
                <FormLabel className="hstack">
                    <div>Logs directory</div>
                    <ConditionalPopover
                        conditions={{
                            isActive: isVisible,
                            message: (
                                <SetupWizardInfoPopover
                                    description={
                                        <ul>
                                            <li>Defines the path to the logs directory.</li>
                                            <li>
                                                By default, RavenDB stores logs in the <code>Logs</code> directory under
                                                the extracted <code>Server</code> folder.
                                            </li>
                                        </ul>
                                    }
                                    ravenLinkHash="BHUAJU"
                                />
                            ),
                        }}
                        popoverPlacement="right"
                    >
                        <Icon icon="info-new" />
                    </ConditionalPopover>
                </FormLabel>
                <FormPathSelector
                    disabled={!isVisible}
                    name="additionalSettingsStep.logsPath"
                    control={control}
                    placeholder={getDefaultPath(os, "logs")}
                    getPathsProvider={(path: string) => getLocalFolderPathsProvider(path)}
                    getPathDependencies={(path: string) => [path]}
                />
                {isVisible && isAbsolutePath(logsPath) && (
                    <RichAlert variant="warning" className="mt-2">
                        This path will be used on all nodes. Please ensure this directory exists on each node, as setup
                        will fail otherwise.
                    </RichAlert>
                )}
            </FormGroup>
            <FormGroup>
                <FormLabel className="hstack">
                    <div>Auto indexing engine type</div>
                    <ConditionalPopover
                        conditions={{
                            isActive: isVisible,
                            message: (
                                <SetupWizardInfoPopover
                                    description="Defines the indexing engine used for auto indexes in RavenDB."
                                    ravenLinkHash="H8IONI"
                                />
                            ),
                        }}
                        popoverPlacement="right"
                    >
                        <Icon icon="info-new" />
                    </ConditionalPopover>
                </FormLabel>
                <FormSelect
                    className={classNames({ "z-n1": !isVisible })}
                    name="additionalSettingsStep.autoIndexingEngineType"
                    control={control}
                    options={setupWizardConstants.indexingEngineTypeOptions}
                />
            </FormGroup>
            <FormGroup>
                <FormLabel className="hstack">
                    <div>Static indexing engine type</div>
                    <ConditionalPopover
                        conditions={{
                            isActive: isVisible,
                            message: (
                                <SetupWizardInfoPopover
                                    description="Defines the indexing engine used for static indexes in RavenDB."
                                    ravenLinkHash="H8IONI"
                                />
                            ),
                        }}
                        popoverPlacement="right"
                    >
                        <Icon icon="info-new" />
                    </ConditionalPopover>
                </FormLabel>
                <FormSelect
                    className={classNames({ "z-n1": !isVisible })}
                    name="additionalSettingsStep.staticIndexingEngineType"
                    control={control}
                    options={setupWizardConstants.indexingEngineTypeOptions}
                />
            </FormGroup>
        </div>
    );
}

export function SetupWizardAdditionalSettingsStepFooter() {
    const { setValue } = useFormContext<SetupWizardFormData>();
    const { reportEvent } = useEventsCollector();

    const handleContinue = () => {
        reportEvent(setupWizardGA4Prefixes.additionalSettingsStep, "continue");
        setValue("currentStep", "Summary");
    };

    const handleBack = () => {
        reportEvent(setupWizardGA4Prefixes.additionalSettingsStep, "back");
        setValue("additionalSettingsStep", setupWizardFormDefaultValues["additionalSettingsStep"]);
        setValue("currentStep", "Node addresses");
    };

    return (
        <div className="hstack justify-content-between">
            <Button variant="secondary" className="rounded-pill" onClick={handleBack}>
                <Icon icon="arrow-left" /> Back
            </Button>
            <Button variant="primary" className="rounded-pill" onClick={handleContinue}>
                Continue <Icon icon="arrow-right" margin="m-0" />
            </Button>
        </div>
    );
}
