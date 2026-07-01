import "./FeatureAvailabilitySummary.scss";
import classNames from "classnames";
import { useRavenLink } from "components/hooks/useRavenLink";
import { useAppSelector } from "components/store";
import { uniqueId } from "lodash";
import { ReactNode } from "react";
import Table from "react-bootstrap/Table";
import Button from "react-bootstrap/Button";
import IconName from "typings/server/icons";
import { licenseSelectors } from "./shell/licenseSlice";
import { Icon } from "./Icon";
import RichAlert from "components/common/RichAlert";
import appUrl from "common/appUrl";
import OverlayTrigger from "react-bootstrap/OverlayTrigger";
import Tooltip from "react-bootstrap/Tooltip";
import PopoverWithHoverWrapper from "./PopoverWithHoverWrapper";
import useBoolean from "components/hooks/useBoolean";
import Modal from "components/common/Modal";

const ravendbLogo = require("Content/img/ravendb_logo.svg");

export type AvailabilityValue = boolean | number | string;

type LicenseTextType = Exclude<Raven.Server.Commercial.LicenseType, "None" | "Reserved"> | "Free" | "Production";

export interface FeatureAvailabilityValueData {
    value: AvailabilityValue;
    overwrittenValue?: AvailabilityValue;
}

export interface FeatureAvailabilityData {
    featureName?: string;
    featureIcon?: IconName;
    helperInfo?: ReactNode;
    community: FeatureAvailabilityValueData;
    professional: FeatureAvailabilityValueData;
    enterprise: FeatureAvailabilityValueData;
    enterpriseAi?: FeatureAvailabilityValueData; // if not set, use enterprise value
}

interface FeatureAvailabilitySummaryProps {
    data: FeatureAvailabilityData[];
}

export function FeatureAvailabilitySummary(props: FeatureAvailabilitySummaryProps) {
    const { data } = props;

    const currentLicense = useAppSelector(licenseSelectors.licenseType);
    const isCloud = useAppSelector(licenseSelectors.statusValue("IsCloud"));
    const buyLink = useRavenLink({ hash: "FLDLO4", isDocs: false });

    const { allLicenseTextTypes, getIsCurrent, getLicenseTypeClass } = useLicenseTextTypes();

    return (
        <>
            {currentLicense === "None" && (
                <RichAlert variant="danger" className="mb-4">
                    No license detected.
                    <br />
                    You are using RavenDB using <strong>AGPL v3 License</strong>.
                    <br />
                    Community license feature restrictions are applied.
                </RichAlert>
            )}
            <div className="feature-availability-table">
                <Table>
                    <thead>
                        <tr>
                            <th className="p-0"></th>
                            {allLicenseTextTypes.map((licenseTextType) => {
                                return (
                                    <th
                                        key={licenseTextType}
                                        className={classNames(
                                            "position-relative",
                                            getLicenseTypeClass(licenseTextType),
                                            {
                                                current: getIsCurrent(licenseTextType),
                                            }
                                        )}
                                    >
                                        <LicenseTitle
                                            licenseTextType={licenseTextType}
                                            licenseClass={getLicenseTypeClass(licenseTextType)}
                                        />
                                        {licenseTextType === "Developer" && (
                                            <PopoverWithHoverWrapper
                                                message={
                                                    <div className="text-center">
                                                        <div>
                                                            Developer license enables{" "}
                                                            <strong>Enterprise License features</strong> but is{" "}
                                                            <strong>not applicable for commercial use</strong>.
                                                        </div>

                                                        <Button
                                                            variant="link"
                                                            size="sm"
                                                            href="https://ravendb.net/l/FLDLO4#developer"
                                                            target="_blank"
                                                        >
                                                            See details <Icon icon="newtab" margin="ms-1" />
                                                        </Button>
                                                    </div>
                                                }
                                                inline={false}
                                            >
                                                <div className="corner-info">
                                                    <Icon icon="info" margin="m-0" />
                                                </div>
                                            </PopoverWithHoverWrapper>
                                        )}
                                    </th>
                                );
                            })}
                        </tr>
                    </thead>
                    <tbody>
                        {data.map((data, idx) => (
                            <tr key={idx} className="feature-row">
                                <th className="p-0">
                                    {data.featureName && (
                                        <div className="p-2">
                                            {data.featureIcon && <Icon icon={data.featureIcon} />}
                                            {data.featureName}
                                            {data.helperInfo && (
                                                <OverlayTrigger
                                                    placement="top"
                                                    overlay={<Tooltip id={data.featureName}>{data.helperInfo}</Tooltip>}
                                                >
                                                    <div className="d-inline-block">
                                                        <Icon icon="info" color="info" className="ms-1" />
                                                    </div>
                                                </OverlayTrigger>
                                            )}
                                        </div>
                                    )}
                                </th>
                                <td
                                    className={classNames("community", {
                                        "current ":
                                            currentLicense === "Community" ||
                                            currentLicense === "Essential" ||
                                            currentLicense === "None",
                                    })}
                                >
                                    {formatAvailabilityValue(data.community)}
                                </td>
                                {!isCloud && (
                                    <td
                                        className={classNames("professional", {
                                            "current ": currentLicense === "Professional",
                                        })}
                                    >
                                        {formatAvailabilityValue(data.professional)}
                                    </td>
                                )}
                                <td
                                    className={classNames("enterprise", {
                                        "current ": currentLicense === "Enterprise",
                                    })}
                                >
                                    {formatAvailabilityValue(data.enterprise, isCloud)}
                                </td>
                                {!isCloud && (
                                    <td
                                        className={classNames("enterprise-ai", {
                                            current: currentLicense === "EnterpriseAi",
                                        })}
                                    >
                                        {formatAvailabilityValue(data.enterpriseAi ?? data.enterprise, isCloud)}
                                    </td>
                                )}
                                {currentLicense === "Developer" && (
                                    <td
                                        className={classNames("developer", {
                                            current: currentLicense === "Developer",
                                        })}
                                    >
                                        {formatAvailabilityValue(data.enterpriseAi ?? data.enterprise, isCloud)}
                                    </td>
                                )}
                            </tr>
                        ))}
                        <tr className="current-indicator-row">
                            <th className="p-0"></th>
                            {allLicenseTextTypes.map((licenseType) => {
                                if (getIsCurrent(licenseType)) {
                                    return (
                                        <td
                                            key={licenseType}
                                            className={classNames("current", getLicenseTypeClass(licenseType), {
                                                "ai-gradient": licenseType === "EnterpriseAi",
                                            })}
                                        >
                                            Current
                                        </td>
                                    );
                                }

                                return <td key={licenseType}></td>;
                            })}
                        </tr>
                    </tbody>
                </Table>
            </div>
            {currentLicense === "None" && (
                <div className="hstack gap-4 justify-content-center mt-4 flex-wrap">
                    <a
                        href={buyLink}
                        target="_blank"
                        color="primary"
                        className="btn btn-primary btn-lg rounded-pill px-4"
                    >
                        <Icon icon="license" margin="me-3" />
                        Get License
                    </a>
                </div>
            )}
        </>
    );
}

type LicenseTitleProps = {
    licenseTextType: LicenseTextType;
    licenseClass: string;
};

function LicenseTitle({ licenseTextType: licenseType, licenseClass }: LicenseTitleProps) {
    const getLicenseTypeTitle = () => {
        if (licenseType === "Developer") {
            return "Dev";
        }
        if (licenseType === "EnterpriseAi") {
            return "AI";
        }

        return licenseType;
    };

    const logoBackgroundStyle: React.CSSProperties =
        licenseType === "EnterpriseAi"
            ? {
                  background: "linear-gradient(135.89deg, #388EE9 0%, #7B51FF 85.4%)",
              }
            : { backgroundColor: `var(--license-${licenseClass})` };

    return (
        <div
            className={classNames("vstack align-items-center", licenseClass, {
                "ai-gradient": licenseType === "EnterpriseAi",
            })}
            style={{ color: `var(--license-${licenseClass})` }}
        >
            <div>
                <div
                    style={{
                        ...logoBackgroundStyle,
                        width: "50px",
                        height: "13px",
                        mask: `url(${ravendbLogo}) no-repeat center`,
                        maskSize: "contain",
                        WebkitMask: `url(${ravendbLogo}) no-repeat center`,
                        WebkitMaskSize: "contain",
                    }}
                />
            </div>
            <div className="fs-5">{getLicenseTypeTitle()}</div>
        </div>
    );
}

export default function FeatureAvailabilitySummaryWrapper({
    data,
    isUnlimited,
    isOpenedByDefault = !isUnlimited,
}: FeatureAvailabilitySummaryProps & { isUnlimited: boolean; isOpenedByDefault?: boolean }) {
    const { value: isOpen, toggle: toggleIsOpen } = useBoolean(isOpenedByDefault);

    return (
        <>
            <div
                className={classNames("license-accordion panel-bg-1 accordion-item", {
                    "license-limited": !isUnlimited,
                })}
                onClick={toggleIsOpen}
            >
                <h2 className="accordion-header">
                    <button type="button" aria-expanded="true" className="accordion-button open-modal-button">
                        <Icon
                            icon="license"
                            color={isUnlimited ? "success" : "warning"}
                            className="me-1 tab-icon me-3 icon-md"
                        ></Icon>
                        <div className="vstack gap-1">
                            <div className="hstack flex-wrap gap-1">
                                <h4 className="m-0">Licensing</h4>
                            </div>
                            <small className="description">
                                See which plans include this feature and other exciting features
                            </small>
                        </div>
                    </button>
                </h2>
            </div>
            {isOpen && <FeatureAvailabilitySummaryModal data={data} toggleIsOpen={toggleIsOpen} />}
        </>
    );
}

function FeatureAvailabilitySummaryModal({
    data,
    toggleIsOpen,
}: FeatureAvailabilitySummaryProps & { toggleIsOpen: () => void }) {
    const licenseType = useAppSelector(licenseSelectors.licenseType);

    return (
        <Modal size="lg" show onHide={toggleIsOpen} contentClassName="modal-border bulge-primary">
            <Modal.Header closeButton onCloseClick={toggleIsOpen}>
                <div>
                    <h3>
                        <Icon icon="license" color="primary" />
                        License comparison
                    </h3>
                    {licenseType !== "Developer" && (
                        <>
                            <br />
                            <div>
                                If you are developing, you can test this and many more features with a free{" "}
                                <a href="https://ravendb.net/license/request/dev" className="text-developer">
                                    Developer license <Icon icon="newtab" margin="m-0" />
                                </a>
                            </div>
                        </>
                    )}
                </div>
            </Modal.Header>
            <Modal.Body className="pt-0">
                <FeatureAvailabilitySummary data={data} />
            </Modal.Body>
            <Modal.Footer className="hstack gap-2 justify-content-end">
                <Button variant="link" onClick={toggleIsOpen} className="link-muted">
                    Close
                </Button>
                {licenseType !== "EnterpriseAi" && licenseType !== "None" && <UpgradeLinkButton />}
            </Modal.Footer>
        </Modal>
    );
}

function UpgradeLinkButton() {
    const isCloud = useAppSelector(licenseSelectors.statusValue("IsCloud"));
    const cloudPricingLink = "https://cloud.ravendb.net/pricing";

    if (isCloud) {
        return (
            <a href={cloudPricingLink} target="_blank" className="btn btn-cloud rounded-pill">
                <Icon icon="cloud" />
                Cloud pricing
            </a>
        );
    }

    return (
        <a href={appUrl.forAbout()} className="btn btn-primary rounded-pill">
            <Icon icon="license" />
            See full comparison
        </a>
    );
}

function formatAvailabilityValue(data: FeatureAvailabilityValueData, canBeEnabledInCloud?: boolean): ReactNode {
    const value = data.overwrittenValue ?? data.value;

    let formattedValue: ReactNode = value;

    if (value === true) {
        formattedValue = <Icon icon="check" margin="m-0" color="success" />;
    }
    if (value === false) {
        if (canBeEnabledInCloud) {
            const cloudOnDemandId = "cloud-on-demand-" + uniqueId();
            return (
                <OverlayTrigger
                    placement="top"
                    overlay={
                        <Tooltip id={cloudOnDemandId}>
                            You can enable this feature in RavenDB Cloud Portal or by contacting support.
                        </Tooltip>
                    }
                >
                    <div className="d-inline-block">
                        <Icon id={cloudOnDemandId} icon="upgrade-arrow" margin="m-0" color="success" />
                    </div>
                </OverlayTrigger>
            );
        } else {
            formattedValue = <Icon icon="cancel" margin="m-0" color="danger" />;
        }
    }
    if (value === Infinity) {
        formattedValue = <Icon icon="infinity" margin="m-0" />;
    }

    if (data.overwrittenValue == null) {
        return formattedValue;
    }

    const id = "overwritten-availability-value-" + uniqueId();

    return (
        <>
            <div className="overwritten-value">
                {formattedValue}
                <PopoverWithHoverWrapper message={`Default value for your license is ${data.value.toString()}.`}>
                    <Icon id={id} icon="info" color="info" margin="m-0" />
                </PopoverWithHoverWrapper>
            </div>
        </>
    );
}

function useLicenseTextTypes() {
    const isCloud = useAppSelector(licenseSelectors.statusValue("IsCloud"));
    const isIsv = useAppSelector(licenseSelectors.statusValue("IsIsv"));
    const currentLicense = useAppSelector(licenseSelectors.licenseType);

    if (!currentLicense) {
        throw new Error("Not expected empty license");
    }

    if (currentLicense === "Reserved") {
        throw new Error("Not expected Reserved license");
    }

    const getAllLicenseTextTypes = () => {
        let licenseTextTypes: LicenseTextType[] = [];

        if (isCloud) {
            licenseTextTypes = ["Free", "Production"];
        } else if (isIsv) {
            licenseTextTypes = ["Essential", "Professional", "Enterprise", "EnterpriseAi"];
        } else {
            licenseTextTypes = ["Community", "Professional", "Enterprise", "EnterpriseAi"];
        }

        if (currentLicense === "Developer") {
            licenseTextTypes.push("Developer");
        }

        return licenseTextTypes;
    };

    const getIsCurrent = (licenseTextType: LicenseTextType) => {
        if (licenseTextType === "Free") {
            return currentLicense === "Community";
        }

        if (licenseTextType === "Production") {
            return currentLicense === "Enterprise";
        }

        if (licenseTextType === "Community" && currentLicense === "None") {
            return true;
        }

        return currentLicense === licenseTextType;
    };

    const getLicenseTypeClass = (licenseTextType: LicenseTextType) => {
        if (licenseTextType === "EnterpriseAi") {
            return "enterprise-ai";
        }

        return licenseTextType.toLowerCase();
    };

    return { allLicenseTextTypes: getAllLicenseTextTypes(), getIsCurrent, getLicenseTypeClass };
}
