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
    RichPanelNameMultiLine,
} from "components/common/RichPanel";
import { accessManagerSelectors } from "components/common/shell/accessManagerSliceSelectors";
import { useEventsCollector } from "components/hooks/useEventsCollector";
import { useServices } from "components/hooks/useServices";
import { ThemeColor } from "components/models/common";
import CertificatesItemStatus from "components/pages/resources/manageServer/certificates/partials/authEnabled/CertificatesItemStatus";
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

    const state = certificatesUtils.getState(certificate.NotAfter, certificate.Disabled);
    const clearance = certificatesUtils.getClearance(certificate.SecurityClearance);
    const isServerCert = certificate.Thumbprints.includes(serverCertificateThumbprint);
    const isServerCertForCommunication = certificate.Thumbprints.includes(serverCertificateForCommunicationThumbprint);
    const isCurrentBrowserCert = certificate.Thumbprints.includes(clientCertificateThumbprint);
    const has2fa = certificate.HasTwoFactor ?? false;

    const isDisabled = certificate.Disabled ?? false;
    const canBeAutomaticallyRenewed = isServerCert && serverCertificateSetupMode === "LetsEncrypt";
    const canEdit = !isServerCert && !isServerCertForCommunication && state !== "Expired";
    const canClone = !isServerCert && !isServerCertForCommunication;
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
            icon: isDisabled ? "unlock" : "disable",
            title: isDisabled
                ? "Do you want to enable this certificate?"
                : "Do you want to disable this certificate?",
            message: (
                <span>
                    Certificate: <strong>{certificate.Name}</strong>
                    <br />
                    Thumbprint: <code>{certificate.Thumbprint}</code>
                </span>
            ),
            actionColor: isDisabled ? "primary" : "warning",
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
            <CertificatesItemStatus state={state} />
            <div className="flex-grow">
                <RichPanelHeader>
                    <div>
                        <RichPanelNameMultiLine className="d-flex align-items-center">
                            {certificate.Name ?? "<empty name>"}
                            {state === "About to expire" && (
                                <Badge
                                    bg="warning"
                                    className="ms-1 fs-6 hstack"
                                    pill
                                    title="This certificate is about to expire"
                                    style={{ gap: "2px" }}
                                >
                                    <Icon icon="clock" margin="m-0" />
                                    About to expire
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
                            {isDisabled && (
                                <Badge bg="danger" className="ms-1 fs-6" pill title="This certificate is disabled">
                                    Disabled
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
                        </RichPanelNameMultiLine>
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
                                onClick={() => dispatch(certificatesActions.cloneModalOpen(certificate))}
                                variant="secondary"
                            >
                                <Icon icon="copy" margin="m-0" />
                            </Button>
                        )}
                        {canEdit && (
                            <Button
                                title="Edit certificate"
                                onClick={() => dispatch(certificatesActions.editModalOpen(certificate))}
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
                                icon={isDisabled ? "unlock" : "lock"}
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
                                <Icon icon="check" />
                                Valid from
                            </>
                        }
                    >
                        {certificate.NotBefore
                            ? moment.utc(certificate.NotBefore).format(genUtils.basicDateFormat)
                            : "Unavailable"}
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
                            {moment.utc(certificate.NotAfter).format(genUtils.basicDateFormat)}
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
                            ? moment.utc(certificate.LastUsedDate).format(genUtils.basicDateFormat)
                            : "(not used)"}
                    </RichPanelDetailItem>
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
