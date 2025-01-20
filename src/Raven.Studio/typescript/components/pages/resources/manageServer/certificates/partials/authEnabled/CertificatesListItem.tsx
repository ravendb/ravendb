import { Icon } from "components/common/Icon";
import {
    RichPanel,
    RichPanelActions,
    RichPanelDetailItem,
    RichPanelDetails,
    RichPanelHeader,
    RichPanelName,
    RichPanelStatus,
} from "components/common/RichPanel";
import { accessManagerSelectors } from "components/common/shell/accessManagerSliceSelectors";
import { TextColor } from "components/models/common";
import { certificatesSelectors } from "components/pages/resources/manageServer/certificates/store/certificatesSliceSelectors";
import { CertificateItem } from "components/pages/resources/manageServer/certificates/utils/certificatesTypes";
import { certificateUtils } from "components/pages/resources/manageServer/certificates/utils/certificatesUtils";
import { useAppSelector } from "components/store";
import assertUnreachable from "components/utils/assertUnreachable";
import moment from "moment";
import { Badge, Button } from "reactstrap";
import IconName from "typings/server/icons";

interface CertificatesListItemProps {
    certificate: CertificateItem;
}

export default function CertificatesListItem({ certificate }: CertificatesListItemProps) {
    const serverCertificateThumbprint = useAppSelector(certificatesSelectors.serverCertificateThumbprint);
    const serverCertificateSetupMode = useAppSelector(certificatesSelectors.serverCertificateSetupMode);
    const serverCertificateRenewalDate = useAppSelector(certificatesSelectors.serverCertificateRenewalDate);

    const clientCertificateThumbprint = useAppSelector(accessManagerSelectors.clientCertificateThumbprint);
    const isClusterAdminOrClusterNode = useAppSelector(accessManagerSelectors.isClusterAdminOrClusterNode);

    const state = certificateUtils.getState(certificate.NotAfter);
    const clearance = certificateUtils.getClearance(certificate.SecurityClearance);
    const isServerCert = certificate.Thumbprints.includes(serverCertificateThumbprint);
    const isCurrentBrowserCert = certificate.Thumbprints.includes(clientCertificateThumbprint);
    const has2fa = certificate.HasTwoFactor ?? false;
    const canBeAutomaticallyRenewed = isServerCert && serverCertificateSetupMode === "LetsEncrypt";

    const canDelete = (() => {
        if (isServerCert) {
            return false;
        }

        if (!isClusterAdminOrClusterNode && clearance === "Admin") {
            return false;
        }

        return true;
    })();

    return (
        <RichPanel className="flex-row with-status">
            <RichPanelStatus color={certificateUtils.getStateColor(state)}>{state}</RichPanelStatus>

            <div className="flex-grow">
                <RichPanelHeader>
                    <div>
                        <RichPanelName className="d-flex align-items-center">
                            <Icon icon="certificate" color="primary" />
                            {certificate.Name}
                            {isServerCert && (
                                <Badge color="info" className="ms-1 fs-6" pill>
                                    Server
                                </Badge>
                            )}
                            {isCurrentBrowserCert && (
                                <Badge color="success" className="ms-1 fs-6" pill>
                                    Current browser
                                </Badge>
                            )}
                            {has2fa && (
                                <Badge color="faded-info" className="ms-1 fs-6" pill>
                                    2FA
                                </Badge>
                            )}
                        </RichPanelName>
                        {certificate.Thumbprint}
                    </div>
                    <RichPanelActions>
                        {state === "About to expire" && (
                            <Button title="Regenerate certificate" color="warning">
                                <Icon icon="refresh" />
                                Regenerate
                            </Button>
                        )}
                        {!isServerCert && (
                            <Button title="Edit certificate">
                                <Icon icon="edit" margin="m-0" />
                            </Button>
                        )}
                        {canDelete && (
                            <Button title="Delete certificate" color="danger">
                                <Icon icon="trash" margin="m-0" />
                            </Button>
                        )}
                    </RichPanelActions>
                </RichPanelHeader>
                <RichPanelDetails>
                    <RichPanelDetailItem
                        label={
                            <>
                                <Icon icon="user" />
                                Security clearance
                            </>
                        }
                    >
                        {getFormattedSecurityClearance(certificate.SecurityClearance)}
                    </RichPanelDetailItem>
                    <RichPanelDetailItem
                        label={
                            <>
                                <Icon icon="star" />
                                Valid from
                            </>
                        }
                    >
                        {moment.utc(certificate.NotBefore).format("YYYY-MM-DD")}
                    </RichPanelDetailItem>
                    <RichPanelDetailItem
                        label={
                            <>
                                <Icon icon="expiration" />
                                {state === "Expired" ? "Expired at" : "Expiration"}
                            </>
                        }
                    >
                        <span className={`text-${certificateUtils.getStateDateColor(state)}`}>
                            {moment.utc(certificate.NotAfter).format("YYYY-MM-DD")}
                        </span>
                    </RichPanelDetailItem>
                    <RichPanelDetailItem
                        label={
                            <>
                                <Icon icon="user-info" />
                                Last used
                            </>
                        }
                    >
                        {certificate.LastUsedDate
                            ? moment.utc(certificate.LastUsedDate).format("YYYY-MM-DD")
                            : "(not used)"}
                    </RichPanelDetailItem>
                    {canBeAutomaticallyRenewed && (
                        <RichPanelDetailItem
                            label={
                                <>
                                    <Icon icon="user" />
                                    Auto renewal date
                                </>
                            }
                        >
                            {moment.utc(serverCertificateRenewalDate).format("YYYY-MM-DD")}
                            <Button color="link" size="sm" className="fs-6">
                                <Icon icon="refresh" />
                                Renew now
                            </Button>
                        </RichPanelDetailItem>
                    )}
                    <RichPanelDetailItem
                        label={
                            <>
                                <Icon icon="database" />
                                Database permissions
                            </>
                        }
                    >
                        {Object.keys(certificate.Permissions).length === 0 ? (
                            <Badge color="faded-success" pill>
                                <Icon icon="user" />
                                All
                            </Badge>
                        ) : (
                            <div className="hstack gap-1">
                                {Object.keys(certificate.Permissions).map((databaseName) => (
                                    <Badge
                                        key={databaseName}
                                        color={getAccessColor(certificate.Permissions[databaseName])}
                                        pill
                                    >
                                        <Icon icon={getAccessIcon(certificate.Permissions[databaseName])} />
                                        {databaseName}
                                    </Badge>
                                ))}
                            </div>
                        )}
                    </RichPanelDetailItem>
                </RichPanelDetails>
            </div>
        </RichPanel>
    );
}

function getAccessIcon(access: Raven.Client.ServerWide.Operations.Certificates.DatabaseAccess): IconName {
    switch (access) {
        case "Admin":
            return "access-admin";
        case "Read":
            return "access-read";
        case "ReadWrite":
            return "access-read-write";
        default:
            assertUnreachable(access);
    }
}

function getAccessColor(access: Raven.Client.ServerWide.Operations.Certificates.DatabaseAccess): `faded-${TextColor}` {
    switch (access) {
        case "Admin":
            return "faded-success";
        case "Read":
            return "faded-danger";
        case "ReadWrite":
            return "faded-warning";
        default:
            assertUnreachable(access);
    }
}

function getFormattedSecurityClearance(
    securityClearance: Raven.Client.ServerWide.Operations.Certificates.SecurityClearance
): string {
    switch (securityClearance) {
        case "ClusterAdmin":
            return "Cluster Admin";
        case "ClusterNode":
            return "ClusterNode";
        case "Operator":
            return "Operator";
        case "ValidUser":
            return "User";
        default:
            return "Unknown";
    }
}
