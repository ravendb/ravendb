import CertificatesClientList from "components/pages/resources/manageServer/certificates/partials/authEnabled/CertificatesClientList";
import CertificatesServerList from "components/pages/resources/manageServer/certificates/partials/authEnabled/CertificatesServerList";
import { certificatesActions } from "components/pages/resources/manageServer/certificates/store/certificatesSlice";
import { certificatesSelectors } from "components/pages/resources/manageServer/certificates/store/certificatesSliceSelectors";
import { useAppDispatch, useAppSelector } from "components/store";
import { useCallback, useEffect } from "react";
import CertificatesGenerateModal from "components/pages/resources/manageServer/certificates/partials/authEnabled/CertificatesGenerateModal";
import CertificatesUploadModal from "components/pages/resources/manageServer/certificates/partials/authEnabled/CertificatesUploadModal";
import CertificatesReplaceServerModal from "components/pages/resources/manageServer/certificates/partials/authEnabled/CertificatesReplaceServerModal";
import { StickyHeader } from "components/common/StickyHeader";
import CertificatesCloneModal from "components/pages/resources/manageServer/certificates/partials/authEnabled/CertificatesCloneModal";
import CertificatesEditModal from "components/pages/resources/manageServer/certificates/partials/authEnabled/CertificatesEditModal";
import CertificatesWellKnownList from "components/pages/resources/manageServer/certificates/partials/authEnabled/CertificatesWellKnownList";
import CertificatesSsoList from "components/pages/resources/manageServer/certificates/partials/authEnabled/CertificatesSsoList";
import { useChanges } from "components/hooks/useChanges";
import { LoadingView } from "components/common/LoadingView";
import { LoadError } from "components/common/LoadError";
import CertificatesFilters from "components/pages/resources/manageServer/certificates/partials/authEnabled/CertificatesFilters";
import CertificatesManageDropdown from "components/pages/resources/manageServer/certificates/partials/authEnabled/CertificatesManageDropdown";
import CertificatesRegisterSsoServerModal from "components/pages/resources/manageServer/certificates/partials/authEnabled/CertificatesRegisterSsoServerModal";
import CertificatesRegisterSsoUserModal from "components/pages/resources/manageServer/certificates/partials/authEnabled/CertificatesRegisterSsoUserModal";
import { EmptySet } from "components/common/EmptySet";

export default function CertificatesAuthEnabled() {
    const dispatch = useAppDispatch();
    const isInitialLoad = useAppSelector(certificatesSelectors.isInitialLoad);
    const loadStatus = useAppSelector(certificatesSelectors.loadStatus);
    const isGenerateModalOpen = useAppSelector(certificatesSelectors.isGenerateModalOpen);
    const isUploadModalOpen = useAppSelector(certificatesSelectors.isUploadModalOpen);
    const isReplaceServerModalOpen = useAppSelector(certificatesSelectors.isReplaceServerModalOpen);
    const certificateToEdit = useAppSelector(certificatesSelectors.certificateToEdit);
    const certificateToClone = useAppSelector(certificatesSelectors.certificateToClone);
    const isRegisterSsoServerModalOpen = useAppSelector(certificatesSelectors.isRegisterSsoServerModalOpen);
    const isRegisterSsoUserModalOpen = useAppSelector(certificatesSelectors.isRegisterSsoUserModalOpen);
    const hasActiveFilter = useAppSelector(certificatesSelectors.hasActiveFilter);
    const filteredCertificates = useAppSelector(certificatesSelectors.filteredCertificates);

    // Initial load
    useEffect(() => {
        dispatch(certificatesActions.fetchData());
    }, [dispatch]);

    const { serverNotifications } = useChanges();

    const handleAlert = useCallback(
        (alert: Raven.Server.NotificationCenter.Notifications.AlertRaised) => {
            if (
                alert.Reason === "Certificates_ReplaceError" ||
                alert.Reason === "Certificates_ReplaceSuccess" ||
                alert.Reason === "Certificates_EntireClusterReplaceSuccess"
            ) {
                dispatch(certificatesActions.fetchData());
            }
        },
        [dispatch]
    );

    useEffect(() => {
        if (serverNotifications) {
            const watchAllAlerts = serverNotifications.watchAllAlerts((e) => handleAlert(e));

            return () => {
                watchAllAlerts.off();
            };
        }
    }, [handleAlert, serverNotifications]);

    return (
        <div className="vstack gap-2 pb-4">
            <StickyHeader>
                {!isInitialLoad && <CertificatesManageDropdown />}
                <CertificatesFilters />
            </StickyHeader>
            {isInitialLoad && loadStatus === "loading" && <LoadingView />}
            {loadStatus === "failure" && (
                <LoadError
                    error="Unable to load certificates"
                    refresh={() => dispatch(certificatesActions.fetchData())}
                />
            )}
            {!isInitialLoad && (
                <>
                    <CertificatesWellKnownList />
                    <CertificatesSsoList />
                    <CertificatesServerList />
                    <CertificatesClientList />
                    {hasActiveFilter && filteredCertificates.length === 0 && (
                        <EmptySet>No certificates match the current filters</EmptySet>
                    )}
                </>
            )}

            {/* Action modals */}
            {isGenerateModalOpen && <CertificatesGenerateModal />}
            {isUploadModalOpen && <CertificatesUploadModal />}
            {isReplaceServerModalOpen && <CertificatesReplaceServerModal />}
            {certificateToClone && <CertificatesCloneModal />}
            {certificateToEdit && <CertificatesEditModal />}
            {isRegisterSsoServerModalOpen && <CertificatesRegisterSsoServerModal />}
            {isRegisterSsoUserModalOpen && <CertificatesRegisterSsoUserModal />}
        </div>
    );
}
