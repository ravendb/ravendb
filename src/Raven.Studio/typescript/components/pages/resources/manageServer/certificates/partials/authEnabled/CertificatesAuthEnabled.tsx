import { Icon } from "components/common/Icon";
import { MultiCheckboxToggle } from "components/common/toggles/MultiCheckboxToggle";
import Select, {
    OptionWithIconAndSeparator,
    SelectOption,
    SelectOptionWithIconAndSeparator,
    SingleValueWithIcon,
} from "components/common/select/Select";
import { databaseSelectors } from "components/common/shell/databaseSliceSelectors";
import CertificatesClientList from "components/pages/resources/manageServer/certificates/partials/authEnabled/CertificatesClientList";
import CertificatesServerList from "components/pages/resources/manageServer/certificates/partials/authEnabled/CertificatesServerList";
import { certificatesActions } from "components/pages/resources/manageServer/certificates/store/certificatesSlice";
import { certificatesSelectors } from "components/pages/resources/manageServer/certificates/store/certificatesSliceSelectors";
import { CertificatesSortMode } from "components/pages/resources/manageServer/certificates/utils/certificatesTypes";
import { useAppDispatch, useAppSelector } from "components/store";
import { useCallback, useEffect, useRef } from "react";
import { Button, DropdownItem, DropdownMenu, DropdownToggle, Input, UncontrolledDropdown } from "reactstrap";
import endpoints from "endpoints";
import CertificatesGenerateModal from "components/pages/resources/manageServer/certificates/partials/authEnabled/CertificatesGenerateModal";
import { ConditionalPopover } from "components/common/ConditionalPopover";
import useDebouncedInput from "components/hooks/useDebouncedInput";
import CertificatesUploadModal from "components/pages/resources/manageServer/certificates/partials/authEnabled/CertificatesUploadModal";
import CertificatesReplaceServerModal from "components/pages/resources/manageServer/certificates/partials/authEnabled/CertificatesReplaceServerModal";
import { StickyHeader } from "components/common/StickyHeader";
import CertificatesRegenerateModal from "components/pages/resources/manageServer/certificates/partials/authEnabled/CertificatesRegenerateModal";
import CertificatesCloneModal from "components/pages/resources/manageServer/certificates/partials/authEnabled/CertificatesCloneModal";
import CertificatesEditModal from "components/pages/resources/manageServer/certificates/partials/authEnabled/CertificatesEditModal";
import CertificatesWellKnownList from "components/pages/resources/manageServer/certificates/partials/authEnabled/CertificatesWellKnownList";
import { useChanges } from "components/hooks/useChanges";
import { useEventsCollector } from "components/hooks/useEventsCollector";

export default function CertificatesAuthEnabled() {
    const dispatch = useAppDispatch();
    const { reportEvent } = useEventsCollector();

    const exportServerCertFormRef = useRef<HTMLFormElement>(null);

    const isGenerateModalOpen = useAppSelector(certificatesSelectors.isGenerateModalOpen);
    const isUploadModalOpen = useAppSelector(certificatesSelectors.isUploadModalOpen);
    const isReplaceServerModalOpen = useAppSelector(certificatesSelectors.isReplaceServerModalOpen);
    const hasClusterNodeCertificate = useAppSelector(certificatesSelectors.hasClusterNodeCertificate);
    const nameOrThumbprintFilter = useAppSelector(certificatesSelectors.nameOrThumbprintFilter);
    const allCertificatesCount = useAppSelector(certificatesSelectors.certificates).length;
    const clearanceFilter = useAppSelector(certificatesSelectors.clearanceFilter);
    const clearanceFilterOptions = useAppSelector(certificatesSelectors.clearanceFilterOptions);
    const stateFilter = useAppSelector(certificatesSelectors.stateFilter);
    const stateFilterOptions = useAppSelector(certificatesSelectors.stateFilterOptions);
    const databaseFilter = useAppSelector(certificatesSelectors.databaseFilter);
    const databaseOptions: SelectOption[] = useAppSelector(databaseSelectors.allDatabaseNames).map((x) => ({
        value: x,
        label: x,
    }));
    const sortMode = useAppSelector(certificatesSelectors.sortMode);

    const certificateToRegenerate = useAppSelector(certificatesSelectors.certificateToRegenerate);
    const certificateToEdit = useAppSelector(certificatesSelectors.certificateToEdit);
    const certificateToClone = useAppSelector(certificatesSelectors.certificateToClone);

    // Initial load
    useEffect(() => {
        dispatch(certificatesActions.fetchData());
    }, [dispatch]);

    const { serverNotifications } = useChanges();

    const handleAlert = useCallback(
        (alert: Raven.Server.NotificationCenter.Notifications.AlertRaised) => {
            if (
                alert.AlertType === "Certificates_ReplaceError" ||
                alert.AlertType === "Certificates_ReplaceSuccess" ||
                alert.AlertType === "Certificates_EntireClusterReplaceSuccess"
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

    const { localValue: nameOrThumbprintFilterInputValue, handleChange: nameOrThumbprintFilterInputHandleChange } =
        useDebouncedInput({
            value: nameOrThumbprintFilter,
            onDebouncedUpdate: (value: string) => dispatch(certificatesActions.nameOrThumbprintFilterSet(value)),
        });

    return (
        <div className="vstack gap-2">
            <StickyHeader>
                <UncontrolledDropdown>
                    <DropdownToggle color="primary" caret className="rounded-pill">
                        Manage certificates
                    </DropdownToggle>
                    <DropdownMenu>
                        <DropdownItem header>Client</DropdownItem>
                        <DropdownItem onClick={() => dispatch(certificatesActions.isGenerateModalOpenToggled())}>
                            <Icon icon="certificate" addon="plus" />
                            Generate client certificate
                        </DropdownItem>
                        <DropdownItem onClick={() => dispatch(certificatesActions.isUploadModalOpenToggled())}>
                            <Icon icon="upload" />
                            Upload client certificate
                        </DropdownItem>
                        <DropdownItem divider />
                        <DropdownItem header>Server</DropdownItem>
                        <ConditionalPopover
                            conditions={[
                                {
                                    isActive: !hasClusterNodeCertificate,
                                    message: "You need to have a server certificate to export it",
                                },
                                {
                                    isActive: true,
                                    message: (
                                        <span>
                                            Export the server certificate(s) without their private key into a .pfx file.
                                            These certificates can be used during a manual cluster setup, when you need
                                            to register server certificates to be trusted on other nodes.
                                        </span>
                                    ),
                                },
                            ]}
                        >
                            <DropdownItem
                                onClick={() => {
                                    reportEvent("certificates", "export-certs");
                                    exportServerCertFormRef.current?.submit();
                                }}
                                disabled={!hasClusterNodeCertificate}
                            >
                                <Icon icon="download" />
                                Export server certificate
                            </DropdownItem>
                        </ConditionalPopover>
                        <ConditionalPopover
                            conditions={{
                                isActive: !hasClusterNodeCertificate,
                                message: "You need to have a server certificate to replace it",
                            }}
                        >
                            <DropdownItem
                                onClick={() => dispatch(certificatesActions.isReplaceServerModalOpenToggled())}
                                disabled={!hasClusterNodeCertificate}
                            >
                                <Icon icon="refresh" />
                                Replace server certificate
                            </DropdownItem>
                        </ConditionalPopover>
                    </DropdownMenu>
                </UncontrolledDropdown>
                <div className="hstack gap-2 mt-2 flex-wrap">
                    <div className="flex-grow">
                        <span className="small-label">Filter by name/thumbprint</span>

                        <div className="clearable-input">
                            <Input
                                onChange={(x) => nameOrThumbprintFilterInputHandleChange(x.target.value)}
                                value={nameOrThumbprintFilterInputValue}
                                placeholder="e.g. johndoe.certificate"
                                className="rounded-pill pe-4"
                            />
                            {nameOrThumbprintFilter && (
                                <div className="clear-button">
                                    <Button
                                        color="secondary"
                                        size="sm"
                                        onClick={() => nameOrThumbprintFilterInputHandleChange("")}
                                    >
                                        <Icon icon="clear" margin="m-0" />
                                    </Button>
                                </div>
                            )}
                        </div>
                    </div>
                    <div>
                        <span className="small-label">Filter by database</span>
                        <Select<SelectOption>
                            options={databaseOptions}
                            onChange={(x) => dispatch(certificatesActions.databaseFilterSet(x?.value))}
                            value={databaseOptions.find((x) => x.value === databaseFilter)}
                            className="rounded-pill"
                            placeholder="Select a database"
                            isRoundedPill
                            isClearable
                        />
                    </div>
                    <div>
                        <span className="small-label">Filter by security clearance</span>
                        <MultiCheckboxToggle
                            inputItems={clearanceFilterOptions}
                            selectedItems={clearanceFilter}
                            setSelectedItems={(x) => {
                                dispatch(certificatesActions.clearanceFilterSet(x));
                            }}
                            selectAll
                            selectAllLabel="All"
                            selectAllCount={allCertificatesCount}
                        />
                    </div>
                    <div>
                        <span className="small-label">Filter by state</span>
                        <MultiCheckboxToggle
                            inputItems={stateFilterOptions}
                            selectedItems={stateFilter}
                            setSelectedItems={(x) => {
                                dispatch(certificatesActions.stateFilterSet(x));
                            }}
                            selectAll
                            selectAllLabel="All"
                            selectAllCount={allCertificatesCount}
                        />
                    </div>
                    <div style={{ minWidth: 250 }}>
                        <span className="small-label">Sort</span>
                        <Select<SelectOptionWithIconAndSeparator<CertificatesSortMode>>
                            options={sortOptions}
                            onChange={(x) => dispatch(certificatesActions.sortModeSet(x.value))}
                            value={sortOptions.find((x) => x.value === sortMode)}
                            className="rounded-pill"
                            placeholder="Select a database"
                            components={{ Option: OptionWithIconAndSeparator, SingleValue: SingleValueWithIcon }}
                            isRoundedPill
                        />
                    </div>
                </div>
            </StickyHeader>

            <CertificatesWellKnownList />
            <CertificatesServerList />
            <CertificatesClientList />

            {/* Action modals */}
            {isGenerateModalOpen && <CertificatesGenerateModal />}
            {isUploadModalOpen && <CertificatesUploadModal />}
            {isReplaceServerModalOpen && <CertificatesReplaceServerModal />}
            {certificateToRegenerate && <CertificatesRegenerateModal />}
            {certificateToClone && <CertificatesCloneModal />}
            {certificateToEdit && <CertificatesEditModal />}

            {/* This form is used to export server certificate */}
            <form
                ref={exportServerCertFormRef}
                action={endpoints.global.adminCertificates.adminCertificatesExport}
                className="d-none"
            />
        </div>
    );
}

const sortOptions: SelectOptionWithIconAndSeparator<CertificatesSortMode>[] = (
    [
        { value: "Default", horizontalSeparatorLine: true },
        { value: "By Name - Asc", icon: "arrow-up" },
        { value: "By Name - Desc", horizontalSeparatorLine: true },
        { value: "By Expiration Date - Asc", icon: "arrow-up" },
        {
            value: "By Expiration Date - Desc",
            icon: "arrow-down",
            horizontalSeparatorLine: true,
        },
        { value: "By Valid-From Date - Asc", icon: "arrow-up" },
        {
            value: "By Valid-From Date - Desc",
            icon: "arrow-down",
            horizontalSeparatorLine: true,
        },
        { value: "By Last Used Date - Asc", icon: "arrow-up" },
        {
            value: "By Last Used Date - Desc",
            icon: "arrow-down",
        },
    ] satisfies Omit<SelectOptionWithIconAndSeparator<CertificatesSortMode>, "label">[]
).map((x) => ({
    ...x,
    label: x.value,
}));
