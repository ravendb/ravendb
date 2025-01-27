import { sortBy } from "common/typeUtils";
import ButtonWithSpinner from "components/common/ButtonWithSpinner";
import useConfirm from "components/common/ConfirmDialog";
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
import { useEventsCollector } from "components/hooks/useEventsCollector";
import { useServices } from "components/hooks/useServices";
import { TextColor } from "components/models/common";
import { certificatesActions } from "components/pages/resources/manageServer/certificates/store/certificatesSlice";
import { certificatesSelectors } from "components/pages/resources/manageServer/certificates/store/certificatesSliceSelectors";
import { CertificateItem } from "components/pages/resources/manageServer/certificates/utils/certificatesTypes";
import { certificatesUtils } from "components/pages/resources/manageServer/certificates/utils/certificatesUtils";
import { useAppDispatch, useAppSelector } from "components/store";
import assertUnreachable from "components/utils/assertUnreachable";
import moment from "moment";
import { useAsyncCallback } from "react-async-hook";
import { Badge, Button } from "reactstrap";
import IconName from "typings/server/icons";

interface CertificatesListItemProps {
    certificate: CertificateItem;
}

export default function CertificatesListItem({ certificate }: CertificatesListItemProps) {
    const { manageServerService } = useServices();
    const dispatch = useAppDispatch();
    const confirm = useConfirm();
    const { reportEvent } = useEventsCollector();

    const serverCertificateThumbprint = useAppSelector(certificatesSelectors.serverCertificateThumbprint);
    const serverCertificateSetupMode = useAppSelector(certificatesSelectors.serverCertificateSetupMode);
    const serverCertificateRenewalDate = useAppSelector(certificatesSelectors.serverCertificateRenewalDate);
    const clientCertificateThumbprint = useAppSelector(accessManagerSelectors.clientCertificateThumbprint);
    const isClusterAdminOrClusterNode = useAppSelector(accessManagerSelectors.isClusterAdminOrClusterNode);

    const state = certificatesUtils.getState(certificate.NotAfter);
    const clearance = certificatesUtils.getClearance(certificate.SecurityClearance);
    const isServerCert = certificate.Thumbprints.includes(serverCertificateThumbprint);
    const isCurrentBrowserCert = certificate.Thumbprints.includes(clientCertificateThumbprint);
    const has2fa = certificate.HasTwoFactor ?? false;

    const canBeAutomaticallyRenewed = isServerCert && serverCertificateSetupMode === "LetsEncrypt";
    const canEdit = !isServerCert && state !== "Expired";
    const canClone = !isServerCert;
    const canRegenerate = !isServerCert && (state === "Expired" || state === "About to expire");
    const canDelete = (() => {
        if (isServerCert) {
            return false;
        }
        if (!isClusterAdminOrClusterNode && clearance === "Admin") {
            return false;
        }
        return true;
    })();

    const asyncRenewServerCertificate = useAsyncCallback(async () => {
        reportEvent("certificates", "renew");
        await manageServerService.forceRenewServerCertificate();
        await dispatch(certificatesActions.fetchData());
    });

    const handleRenewServerCertificate = async () => {
        const isConfirmed = await confirm({
            icon: "refresh",
            title: "Do you want to renew the server certificate?",
            actionColor: "primary",
            confirmText: "Renew certificate",
        });

        if (isConfirmed) {
            asyncRenewServerCertificate.execute();
        }
    };

    const asyncDeleteCertificate = useAsyncCallback(async () => {
        reportEvent("certificates", "delete");
        await manageServerService.deleteCertificate(certificate.Thumbprint);
        await dispatch(certificatesActions.fetchData());
    });

    const handleDeleteCertificate = async () => {
        const isConfirmed = await confirm({
            icon: "trash",
            title: "Do you want to delete certificate?",
            message: (
                <span>
                    Thumbprint: <code>{certificate.Thumbprint}</code>
                </span>
            ),
            actionColor: "danger",
            confirmText: "Delete certificate",
        });

        if (isConfirmed) {
            asyncDeleteCertificate.execute();
        }
    };

    return (
        <RichPanel className="flex-row with-status">
            <RichPanelStatus color={certificatesUtils.getStateColor(state)}>{state}</RichPanelStatus>

            <div className="flex-grow">
                <RichPanelHeader>
                    <div>
                        <RichPanelName className="d-flex align-items-center">
                            <Icon icon="certificate" color="primary" />
                            {certificate.Name ?? "<empty name>"}
                            {isServerCert && (
                                <Badge
                                    color="info"
                                    className="ms-1 fs-6"
                                    pill
                                    title="This certificate is currently used by the server for incoming HTTPS connections"
                                >
                                    Server
                                </Badge>
                            )}
                            {isCurrentBrowserCert && (
                                <Badge
                                    color="success"
                                    className="ms-1 fs-6"
                                    pill
                                    title="This is the client certificate currently used by the browser"
                                >
                                    Current browser
                                </Badge>
                            )}
                            {has2fa && (
                                <Badge
                                    color="faded-info"
                                    className="ms-1 fs-6"
                                    pill
                                    title="This is the certificate which requires two-factor authentication"
                                >
                                    2FA
                                </Badge>
                            )}
                        </RichPanelName>
                        {certificate.Thumbprints.join(", ")}
                    </div>
                    <RichPanelActions>
                        {canRegenerate && (
                            <Button
                                title="Regenerate certificate"
                                color="warning"
                                onClick={() => dispatch(certificatesActions.regenerateModalOpen(certificate))}
                            >
                                <Icon icon="refresh" />
                                Regenerate
                            </Button>
                        )}
                        {canClone && (
                            <Button
                                title="Clone certificate"
                                onClick={() => dispatch(certificatesActions.cloneModalOpen(certificate))}
                            >
                                <Icon icon="copy" margin="m-0" />
                            </Button>
                        )}
                        {canEdit && (
                            <Button
                                title="Edit certificate"
                                onClick={() => dispatch(certificatesActions.editModalOpen(certificate))}
                            >
                                <Icon icon="edit" margin="m-0" />
                            </Button>
                        )}
                        {canDelete && (
                            <ButtonWithSpinner
                                title="Delete certificate"
                                color="danger"
                                onClick={handleDeleteCertificate}
                                icon="trash"
                                isSpinning={asyncDeleteCertificate.loading}
                            />
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
                        {certificate.NotBefore ? moment.utc(certificate.NotBefore).format("YYYY-MM-DD") : "Unavailable"}
                    </RichPanelDetailItem>
                    <RichPanelDetailItem
                        label={
                            <>
                                <Icon icon="expiration" />
                                {state === "Expired" ? "Expired at" : "Expiration"}
                            </>
                        }
                    >
                        <span className={`text-${certificatesUtils.getStateDateColor(state)}`}>
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
                            <ButtonWithSpinner
                                color="link"
                                size="sm"
                                className="fs-6"
                                title="Renew this server certificate"
                                onClick={handleRenewServerCertificate}
                                icon="refresh"
                                isSpinning={asyncRenewServerCertificate.loading}
                            >
                                Renew now
                            </ButtonWithSpinner>
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
                        <PermissionsBadge certificate={certificate} />
                    </RichPanelDetailItem>
                </RichPanelDetails>
            </div>
        </RichPanel>
    );
}

function PermissionsBadge({ certificate }: { certificate: CertificateItem }) {
    const { SecurityClearance, Permissions } = certificate;

    if (
        SecurityClearance === "ClusterNode" ||
        SecurityClearance === "ClusterAdmin" ||
        SecurityClearance === "Operator"
    ) {
        return (
            <Badge color="faded-success" pill>
                <Icon icon="user" />
                All
            </Badge>
        );
    }

    const dbAccessArray = sortBy(
        Object.entries(Permissions ?? []).map(([databaseName, accessLevel]) => ({
            databaseName,
            accessLevel,
        })),
        (x) => x.databaseName.toLowerCase()
    );

    if (dbAccessArray.length === 0) {
        return <span className="text-danger">None</span>;
    }

    return (
        <div className="hstack gap-1">
            {dbAccessArray.map(({ databaseName, accessLevel }) => (
                <Badge key={databaseName} color={getAccessColor(accessLevel)} pill>
                    <Icon icon={getAccessIcon(accessLevel)} />
                    {databaseName}
                </Badge>
            ))}
        </div>
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
