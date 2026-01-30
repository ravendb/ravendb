import { useFormContext, useWatch } from "react-hook-form";
import { SetupWizardFormData, SetupWizardSecurityOption } from "../setupWizardValidation";
import { Icon } from "components/common/Icon";
import Button from "react-bootstrap/Button";
import React from "react";
import Card from "react-bootstrap/Card";
import { DistributionItem, DistributionLegend, LocationDistribution } from "components/common/LocationDistribution";
import PopoverWithHoverWrapper from "components/common/PopoverWithHoverWrapper";
import { getFullDomain, getLicenseType } from "components/setupWizard/utils/setupWizardUtils";
import LicenseType = Raven.Server.Commercial.LicenseType;
import License = Raven.Server.Commercial.License;

export function SetupWizardSummaryStep() {
    const { control } = useFormContext<SetupWizardFormData>();

    const {
        nodeAddressStep,
        additionalSettingsStep,
        domainStep,
        securityStep: { securityOption },
        licenseKeyStep,
    } = useWatch({ control });

    const license: License = licenseKeyStep.key ? JSON.parse(licenseKeyStep.key) : ({} as License);

    const hasAdditionalSettings =
        !!additionalSettingsStep?.logsPath ||
        !!additionalSettingsStep?.dataDirectory ||
        !!additionalSettingsStep?.autoIndexingEngineType ||
        !!additionalSettingsStep?.staticIndexingEngineType ||
        !!additionalSettingsStep?.setupCertificatePath ||
        !!(
            getLicenseType(licenseKeyStep.licenseInfo).isHigherThan("Professional") &&
            additionalSettingsStep?.studioEnvironment
        ) ||
        !!(securityOption !== "none" && additionalSettingsStep?.adminCertificateExpirationTime) ||
        !!additionalSettingsStep?.postgresqlIntegration;

    return (
        <div>
            <div className="mb-5">
                <h2 className="mb-1">Summary</h2>
                <p className="mb-4 text-muted">Check if everything’s correct before proceeding.</p>
            </div>
            <div className="vstack gap-1 mb-2">
                <h5 className="mb-0">Security & license</h5>
                <Card>
                    <Card.Body>
                        <div className="vstack gap-1">
                            <CardRow
                                label="Certificate"
                                valueClassName={securityOption === "none" ? "text-warning" : null}
                                value={getCertificateDescription(securityOption)}
                            />
                            {securityOption !== "none" && <CardRow label="License ID" value={license?.Id} />}
                            <CardRow
                                label="License type"
                                value={licenseKeyStep.licenseInfo?.licenseType ?? "AGPLv3"}
                                valueClassName={colorizeLicenseType(licenseKeyStep.licenseInfo?.licenseType)}
                            />
                            {securityOption === "letsEncrypt" && (
                                <CardRow label="Full domain" value={getFullDomain(domainStep)} />
                            )}
                        </div>
                    </Card.Body>
                </Card>
                <h5 className="mb-0 mt-4">Cluster settings</h5>
                <Card>
                    <Card.Body>
                        <LocationDistribution>
                            <DistributionLegend>
                                <div className="top"></div>
                                <div className="node">HTTPS port</div>
                                <div>TCP port</div>
                                <div>IP address/Hostname</div>
                            </DistributionLegend>
                            {nodeAddressStep.nodes.map((node, index) => (
                                <NodeDistributionItem key={index} {...node} />
                            ))}
                        </LocationDistribution>
                    </Card.Body>
                </Card>
                {hasAdditionalSettings && (
                    <>
                        <h5 className="mb-0 mt-4">Additional & advanced</h5>
                        <Card className="mb-4">
                            <Card.Body>
                                <div className="vstack gap-1">
                                    {getLicenseType(licenseKeyStep.licenseInfo).isHigherThan("Community") && (
                                        <CardRow
                                            label="Studio environment"
                                            value={additionalSettingsStep.studioEnvironment}
                                        />
                                    )}
                                    {securityOption !== "none" && (
                                        <CardRow
                                            label="Admin client certificate expiration time"
                                            value={`${additionalSettingsStep.adminCertificateExpirationTime} months`}
                                        />
                                    )}
                                    {getLicenseType(licenseKeyStep.licenseInfo).isHigherThan("Community") && (
                                        <CardRow
                                            label="Experimental features"
                                            value={
                                                additionalSettingsStep.postgresqlIntegration ? "Enabled" : "Disabled"
                                            }
                                            valueClassName={
                                                additionalSettingsStep.postgresqlIntegration ? "text-success" : null
                                            }
                                        />
                                    )}
                                    {additionalSettingsStep.dataDirectory && (
                                        <CardRow label="Data directory" value={additionalSettingsStep.dataDirectory} />
                                    )}
                                    {additionalSettingsStep.setupCertificatePath && (
                                        <CardRow
                                            label="Setup certificate path"
                                            value={additionalSettingsStep.setupCertificatePath}
                                        />
                                    )}
                                    {additionalSettingsStep.logsPath && (
                                        <CardRow label="Logs path" value={additionalSettingsStep.logsPath} />
                                    )}
                                    {additionalSettingsStep.autoIndexingEngineType && (
                                        <CardRow
                                            label="Auto indexing engine type"
                                            value={additionalSettingsStep.autoIndexingEngineType}
                                        />
                                    )}
                                    {additionalSettingsStep.staticIndexingEngineType && (
                                        <CardRow
                                            label="Static indexing engine type"
                                            value={additionalSettingsStep.staticIndexingEngineType}
                                        />
                                    )}
                                </div>
                            </Card.Body>
                        </Card>
                    </>
                )}
            </div>
        </div>
    );
}

function getCertificateDescription(option: SetupWizardSecurityOption) {
    switch (option) {
        case "letsEncrypt":
            return "Let's Encrypt";
        case "ownCertificate":
            return "Own certificate";
        case "none":
            return "Unsecure";
        default:
            return "Unknown";
    }
}

function colorizeLicenseType(licenseType: LicenseType) {
    switch (licenseType) {
        case "Developer":
            return "text-developer";
        case "Enterprise":
            return "text-enterprise";
        case "Professional":
            return "text-professional";
        case "Community":
        case "Essential":
            return "text-info";
        default:
            return "";
    }
}

interface CardRowProps {
    label: string;
    value: string | number;
    valueClassName?: string;
}

function CardRow({ label, value, valueClassName }: CardRowProps) {
    return (
        <div className="d-flex justify-content-between">
            <span>{label}</span>
            <span className={valueClassName}>{value}</span>
        </div>
    );
}

function NodeDistributionItem({
    nodeTag,
    httpPort,
    tcpPort,
    ipAddress,
    nodeUrl,
    externalHttpPort,
    externalTcpPort,
    externalIpAddress,
}: SetupWizardFormData["nodeAddressStep"]["nodes"][number]) {
    return (
        <DistributionItem>
            <div className="top node">
                <PopoverWithHoverWrapper
                    message={
                        <div>
                            Node {nodeTag} URL: <a href="#">{nodeUrl}</a>
                        </div>
                    }
                >
                    <Icon icon="node" /> {nodeTag}
                </PopoverWithHoverWrapper>
            </div>
            <div>
                <PopoverWithHoverWrapper
                    message={
                        <div>
                            Your node listens on port <strong>{httpPort}</strong> internally
                            {externalHttpPort && (
                                <>
                                    , while external traffic connects via port <strong>{externalHttpPort}</strong>.
                                </>
                            )}
                        </div>
                    }
                >
                    {httpPort} {externalHttpPort && <span> → {externalHttpPort}</span>}
                </PopoverWithHoverWrapper>
            </div>
            <div>
                <PopoverWithHoverWrapper
                    message={
                        <div>
                            Your node listens on port <b>{tcpPort}</b> internally
                            {externalTcpPort && (
                                <>
                                    , while external traffic connects via port <b>{externalTcpPort}</b>.
                                </>
                            )}
                        </div>
                    }
                >
                    {tcpPort} {externalTcpPort && <span> → {externalTcpPort}</span>}
                </PopoverWithHoverWrapper>
            </div>
            <div>
                <PopoverWithHoverWrapper
                    message={
                        <div>
                            Your node is accessible internally via {ipAddress.length === 1 ? "IP" : "multiple IPs"}:
                            <ul className="mb-0">
                                {ipAddress.map((ip) => (
                                    <li key={ip.ipAddress}>
                                        <strong>{ip.ipAddress}</strong>
                                    </li>
                                ))}
                            </ul>
                            {externalIpAddress && (
                                <>
                                    while external traffic reaches it via the public IP:{" "}
                                    <strong>{externalIpAddress}</strong>
                                </>
                            )}
                        </div>
                    }
                >
                    <FormatIpAddresses
                        addresses={ipAddress.map((x) => x.ipAddress)}
                        externalIpAddress={externalIpAddress}
                    />
                </PopoverWithHoverWrapper>
            </div>
        </DistributionItem>
    );
}

interface FormatIpAddressesProps {
    addresses: string[];
    externalIpAddress?: string;
}

function FormatIpAddresses({ addresses, externalIpAddress }: FormatIpAddressesProps) {
    if (addresses.length <= 1) {
        if (externalIpAddress) {
            return (
                <>
                    {addresses[0]} → {externalIpAddress}
                </>
            );
        }

        return <>{addresses[0]}</>;
    }

    const firstAddress = addresses[0];
    const remainingCount = addresses.length - 1;

    if (externalIpAddress) {
        return <>{`${firstAddress} (+${remainingCount} more) → ${externalIpAddress}`}</>;
    }

    return <>{`${firstAddress} (+${remainingCount} more)`}</>;
}

export function SetupWizardSummaryStepFooter() {
    const { setValue } = useFormContext<SetupWizardFormData>();

    const handleContinue = () => {
        setValue("currentStep", "Finish");
    };

    const handleBack = () => {
        setValue("currentStep", "Additional settings");
    };

    return (
        <div className="hstack justify-content-between">
            <Button variant="secondary" className="rounded-pill" onClick={handleBack}>
                <Icon icon="arrow-left" /> Back
            </Button>
            <Button variant="primary" className="rounded-pill" onClick={handleContinue}>
                Finish <Icon icon="arrow-right" margin="m-0" />
            </Button>
        </div>
    );
}
