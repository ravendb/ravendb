import copyToClipboard from "common/copyToClipboard";
import genUtils from "common/generalUtils";
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
} from "components/common/RichPanel";
import { accessManagerSelectors } from "components/common/shell/accessManagerSliceSelectors";
import { useEventsCollector } from "components/hooks/useEventsCollector";
import { useServices } from "components/hooks/useServices";
import { ThemeColor } from "components/models/common";
import CertificatesItemStatus from "components/pages/resources/manageServer/certificates/partials/authEnabled/CertificatesItemStatus";
import PopoverWithHoverWrapper from "components/common/PopoverWithHoverWrapper";
import { certificatesActions } from "components/pages/resources/manageServer/certificates/store/certificatesSlice";
import { certificatesSelectors } from "components/pages/resources/manageServer/certificates/store/certificatesSliceSelectors";
import {
    CertificateItem,
    UpdateCertificateDto,
} from "components/pages/resources/manageServer/certificates/utils/certificatesTypes";
import { certificatesUtils } from "components/pages/resources/manageServer/certificates/utils/certificatesUtils";
import { useAppDispatch, useAppSelector } from "components/store";
import assertUnreachable from "components/utils/assertUnreachable";
import moment from "moment";
import { useAsyncCallback } from "react-async-hook";
import Badge from "react-bootstrap/Badge";
import Button from "react-bootstrap/Button";
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
    const serverCertificateForCommunicationThumbprint = useAppSelector(
        certificatesSelectors.serverCertificateForCommunicationThumbprint
    );
    const serverCertificateSetupMode = useAppSelector(certificatesSelectors.serverCertificateSetupMode);
    const serverCertificateRenewalDate = useAppSelector(certificatesSelectors.serverCertificateRenewalDate);
    const clientCertificateThumbprint = useAppSelector(accessManagerSelectors.clientCertificateThumbprint);
    const isClusterAdminOrClusterNode = useAppSelector(accessManagerSelectors.isClusterAdminOrClusterNode);
    const ssoServerCertificates = useAppSelector(certificatesSelectors.ssoServerCertificates);
    const ssoUserCertificates = useAppSelector(certificatesSelectors.ssoUserCertificates);

    const state = certificate.NotAfter ? certificatesUtils.getState(certificate) : null;
    const clearance = certificatesUtils.getClearance(certificate.SecurityClearance);
    const isServerCert = certificate.Thumbprints.includes(serverCertificateThumbprint);
    const isServerCertForCommunication = certificate.Thumbprints.includes(serverCertificateForCommunicationThumbprint);
    const isCurrentBrowserCert = certificate.Thumbprints.includes(clientCertificateThumbprint);
    const has2fa = certificate.HasTwoFactor ?? false;
    const certDisplayName = certificate.Name ?? "<empty name>";

    const isDisabled = certificate.Disabled ?? false;
    const canBeAutomaticallyRenewed = isServerCert && serverCertificateSetupMode === "LetsEncrypt";
    const authorizedSsoUsers =
        certificate.Usage === "SsoServer"
            ? ssoUserCertificates.filter(
                  (u) =>
                      u.AllowAnySsoServer ||
                      (u.SsoServerPublicKeyPinningHashes ?? []).includes(certificate.PublicKeyPinningHash)
              )
            : [];
    const canEdit =
        !isServerCert && !isServerCertForCommunication && certificate.Usage !== "SsoServer" && state !== "Expired";
    const canClone = !isServerCert && !isServerCertForCommunication && certificate.Usage !== "SsoServer";
    const canToggleDisable =
        !isServerCert &&
        !isServerCertForCommunication &&
        !isCurrentBrowserCert &&
        certificate.SecurityClearance !== "ClusterNode" &&
        (isClusterAdminOrClusterNode || clearance !== "Admin");
    const canDelete = (() => {
        if (isServerCert || isServerCertForCommunication) {
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

    const asyncToggleDisable = useAsyncCallback(async () => {
        const newDisabledState = !isDisabled;
        reportEvent("certificates", newDisabledState ? "disable" : "enable");

        const dto: UpdateCertificateDto = {
            Name: certificate.Name,
            Thumbprint: certificate.Thumbprint,
            SecurityClearance: certificate.SecurityClearance,
            Permissions: certificate.Permissions,
            TwoFactorAuthenticationKey: null,
            Disabled: newDisabledState,
        };

        await manageServerService.updateCertificate(dto, false);
        await dispatch(certificatesActions.fetchData());
    });

    const handleToggleDisable = async () => {
        const isConfirmed = await confirm({
            icon: isDisabled ? "play" : "stop",
            title: isDisabled ? "Do you want to enable this certificate?" : "Do you want to disable this certificate?",
            message: (
                <span>
                    Certificate: <strong>{certificate.Name}</strong>
                    <br />
                    Thumbprint: <code>{certificate.Thumbprint}</code>
                </span>
            ),
            actionColor: isDisabled ? "success" : "warning",
            confirmText: isDisabled ? "Enable certificate" : "Disable certificate",
        });

        if (!isConfirmed) {
            return;
        }

        asyncToggleDisable.execute();
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
        <RichPanel className="flex-row with-status" hover>
            <CertificatesItemStatus state={state ?? "Valid"} />
            <div className="flex-grow">
                <RichPanelHeader>
                    <div className="flex-grow">
                        <div className="d-flex align-items-center justify-content-start flex-wrap w-100">
                            <span className="fs-4 text-truncate" title={certDisplayName} style={{ maxWidth: "400px" }}>
                                {certDisplayName}
                            </span>
                            {state === "About to expire" && (
                                <Badge
                                    bg="warning"
                                    className="ms-1 fs-6"
                                    pill
                                    title="This certificate is about to expire"
                                >
                                    <Icon icon="clock" margin="m-0" /> About to expire
                                </Badge>
                            )}
                            {isCurrentBrowserCert && (
                                <Badge
                                    bg="success"
                                    className="ms-1 fs-6"
                                    pill
                                    title="This is the client certificate currently used by the browser"
                                >
                                    Current browser
                                </Badge>
                            )}
                            {has2fa && (
                                <Badge
                                    bg="2fa"
                                    className="ms-1 fs-6"
                                    pill
                                    title="This is the certificate which requires two-factor authentication"
                                >
                                    2FA
                                </Badge>
                            )}
                            {certificate.Usage === "SsoServer" && (
                                <Badge
                                    bg="info"
                                    className="ms-1 fs-6"
                                    pill
                                    title="This certificate acts as an SSO server"
                                >
                                    SSO Server
                                </Badge>
                            )}
                            {certificate.Usage === "SsoClient" && (
                                <Badge bg="primary" className="ms-1 fs-6" pill>
                                    SSO User
                                </Badge>
                            )}
                        </div>
                        {certificate.Thumbprints.join(", ")}
                        <Button
                            variant="link"
                            size="xs"
                            onClick={() =>
                                copyToClipboard.copy(
                                    certificate.Thumbprints.join(", "),
                                    "Successfully copied thumbprints to clipboard"
                                )
                            }
                        >
                            <Icon icon="copy" margin="m-0" />
                        </Button>
                    </div>
                    <RichPanelActions>
                        {canClone && (
                            <Button
                                title="Clone certificate"
                                onClick={() =>
                                    certificate.Usage === "SsoClient"
                                        ? dispatch(certificatesActions.ssoUserCloneModalOpen(certificate))
                                        : dispatch(certificatesActions.cloneModalOpen(certificate))
                                }
                                variant="secondary"
                            >
                                <Icon icon="copy" margin="m-0" />
                            </Button>
                        )}
                        {canEdit && (
                            <Button
                                title="Edit certificate"
                                onClick={() =>
                                    certificate.Usage === "SsoClient"
                                        ? dispatch(certificatesActions.ssoUserEditModalOpen(certificate))
                                        : dispatch(certificatesActions.editModalOpen(certificate))
                                }
                                variant="secondary"
                            >
                                <Icon icon="edit" margin="m-0" />
                            </Button>
                        )}
                        {canToggleDisable && (
                            <ButtonWithSpinner
                                title={isDisabled ? "Enable certificate" : "Disable certificate"}
                                variant={isDisabled ? "success" : "warning"}
                                onClick={handleToggleDisable}
                                icon={isDisabled ? "play" : "stop"}
                                isSpinning={asyncToggleDisable.loading}
                            />
                        )}
                        {canDelete && (
                            <ButtonWithSpinner
                                title="Delete certificate"
                                variant="danger"
                                onClick={handleDeleteCertificate}
                                icon="trash"
                                isSpinning={asyncDeleteCertificate.loading}
                            />
                        )}
                    </RichPanelActions>
                </RichPanelHeader>
                <RichPanelDetails>
                    {certificate.Usage !== "SsoServer" && (
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
                    )}
                    {certificate.NotBefore && certificate.Usage !== "SsoClient" && (
                        <RichPanelDetailItem
                            label={
                                <>
                                    <Icon icon="check" />
                                    Valid from
                                </>
                            }
                        >
                            {moment.utc(certificate.NotBefore).format(genUtils.basicDateFormat)}
                        </RichPanelDetailItem>
                    )}
                    {certificate.NotAfter && (
                        <RichPanelDetailItem
                            label={
                                <>
                                    <Icon icon="expiration" />
                                    {state === "Expired" ? "Expired at" : "Expiration"}
                                </>
                            }
                        >
                            <span className={`text-${certificatesUtils.getStateDateColor(state)}`}>
                                {moment.utc(certificate.NotAfter).format(genUtils.basicDateFormat)}
                            </span>
                        </RichPanelDetailItem>
                    )}
                    <RichPanelDetailItem
                        label={
                            <>
                                <Icon icon="user-info" />
                                Last used
                            </>
                        }
                    >
                        {certificate.LastUsedDate
                            ? moment.utc(certificate.LastUsedDate).format(genUtils.basicDateFormat)
                            : "(not used)"}
                    </RichPanelDetailItem>
                    {certificate.Usage === "SsoServer" && (
                        <RichPanelDetailItem
                            label={
                                <>
                                    <Icon icon="user" />
                                    Entries authorized
                                </>
                            }
                        >
                            {authorizedSsoUsers.length}
                            {authorizedSsoUsers.length > 0 && (
                                <PopoverWithHoverWrapper
                                    message={
                                        <ul className="mb-0 ps-3">
                                            {authorizedSsoUsers.map((u) => (
                                                <li key={u.Thumbprint}>{u.Name ?? u.Thumbprint}</li>
                                            ))}
                                        </ul>
                                    }
                                >
                                    <Icon icon="info" color="info" margin="ms-1" className="small" />
                                </PopoverWithHoverWrapper>
                            )}
                        </RichPanelDetailItem>
                    )}
                    {certificate.Usage === "SsoClient" && certificate.SsoIdentifiers?.length > 0 && (
                        <RichPanelDetailItem
                            label={
                                <>
                                    <Icon icon="user" />
                                    Identifiers
                                </>
                            }
                        >
                            {certificate.SsoIdentifiers.map((id) =>
                                id.Provider === "Windows" && id.Domain
                                    ? `Windows\\${id.Domain}: ${id.Identifier}`
                                    : `${id.Provider}: ${id.Identifier}`
                            ).join(" · ")}
                        </RichPanelDetailItem>
                    )}
                    {certificate.Usage === "SsoClient" && (
                        <RichPanelDetailItem
                            label={
                                <>
                                    <Icon icon="key" />
                                    Authorizing SSO
                                </>
                            }
                        >
                            {certificate.AllowAnySsoServer
                                ? "Allow any SSO to authorize"
                                : ssoServerCertificates
                                      .filter((s) =>
                                          (certificate.SsoServerPublicKeyPinningHashes ?? []).includes(
                                              s.PublicKeyPinningHash
                                          )
                                      )
                                      .map((s) => s.Name)
                                      .join(", ") || "None"}
                        </RichPanelDetailItem>
                    )}
                    {canBeAutomaticallyRenewed && (
                        <RichPanelDetailItem
                            label={
                                <>
                                    <Icon icon="refresh" />
                                    Auto renewal date
                                </>
                            }
                        >
                            {moment.utc(serverCertificateRenewalDate).format(genUtils.basicDateFormat)}
                            <ButtonWithSpinner
                                variant="link"
                                size="xs"
                                title="Renew this server certificate"
                                onClick={handleRenewServerCertificate}
                                icon="refresh"
                                isSpinning={asyncRenewServerCertificate.loading}
                            >
                                Renew now
                            </ButtonWithSpinner>
                        </RichPanelDetailItem>
                    )}
                    {certificate.Usage !== "SsoServer" && (
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
                    )}
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
            <Badge bg="faded-success" title="Cluster admin access" pill>
                <Icon icon="cluster" />
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
                <Badge key={databaseName} bg={getAccessColor(accessLevel)} title={getAccessTitle(accessLevel)} pill>
                    <Icon icon={getAccessIcon(accessLevel)} />
                    {databaseName}
                </Badge>
            ))}
        </div>
    );
}

function getAccessTitle(access: Raven.Client.ServerWide.Operations.Certificates.DatabaseAccess) {
    switch (access) {
        case "Admin":
            return "Admin access";
        case "Read":
            return "Read access";
        case "ReadWrite":
            return "Read/write access";
        default:
            assertUnreachable(access);
    }
}

function getAccessIcon(access: Raven.Client.ServerWide.Operations.Certificates.DatabaseAccess): IconName {
    switch (access) {
        case "Admin":
            return "hammer-driver";
        case "Read":
            return "access-read";
        case "ReadWrite":
            return "access-read-write";
        default:
            assertUnreachable(access);
    }
}

function getAccessColor(access: Raven.Client.ServerWide.Operations.Certificates.DatabaseAccess): `faded-${ThemeColor}` {
    switch (access) {
        case "Admin":
            return "faded-danger";
        case "Read":
            return "faded-info";
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
