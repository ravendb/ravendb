import viewModelBase = require("viewmodels/viewModelBase");
import databasesManager = require("common/shell/databasesManager");
import certificateModel = require("models/auth/certificateModel");
import getCertificatesCommand = require("commands/auth/getCertificatesCommand");
import certificatePermissionModel = require("models/auth/certificatePermissionModel");
import uploadCertificateCommand = require("commands/auth/uploadCertificateCommand");
import deleteCertificateCommand = require("commands/auth/deleteCertificateCommand");
import replaceClusterCertificateCommand = require("commands/auth/replaceClusterCertificateCommand");
import updateCertificatePermissionsCommand = require("commands/auth/updateCertificatePermissionsCommand");
import getServerCertificateSetupModeCommand = require("commands/auth/getServerCertificateSetupModeCommand");
import forceRenewServerCertificateCommand = require("commands/auth/forceRenewServerCertificateCommand");
import getNextOperationIdCommand = require("commands/database/studio/getNextOperationIdCommand");
import notificationCenter = require("common/notifications/notificationCenter");
import getClusterDomainsCommand = require("commands/auth/getClusterDomainsCommand");
import endpoints = require("endpoints");
import copyToClipboard = require("common/copyToClipboard");
import popoverUtils = require("common/popoverUtils");
import messagePublisher = require("common/messagePublisher");
import eventsCollector = require("common/eventsCollector");
import changesContext = require("common/changesContext");
import accessManager = require("common/shell/accessManager");
import getServerCertificateRenewalDateCommand = require("commands/auth/getServerCertificateRenewalDateCommand");
import fileImporter = require("common/fileImporter");
import generalUtils = require("common/generalUtils");
import moment = require("moment");
import generateTwoFactorSecretCommand from "commands/auth/generateTwoFactorSecretCommand";
import { QRCode } from "qrcodejs";
import clusterTopologyManager from "common/shell/clusterTopologyManager";
import getAdminStatsCommand from "commands/resources/getAdminStatsCommand";
import assertUnreachable from "components/utils/assertUnreachable";
import licenseModel from "models/auth/licenseModel";
import { CertificatesInfoHub } from "viewmodels/manage/CertificatesInfoHub";
import serverSettings from "common/settings/serverSettings";

type certificatesSortMode = "default" |
    "byNameAsc" |
    "byNameDesc" | 
    "byExpirationAsc" |
    "byExpirationDesc" |
    "byValidFromAsc" |
    "byValidFromDesc" | 
    "byLastUsedAsc" |
    "byLastUsedDesc";

interface unifiedCertificateDefinitionWithCache extends unifiedCertificateDefinition {
    expirationClass: string;
    expirationText: string;
    expirationIcon: string;
    isExpired: boolean;
    isAboutToExpire: boolean;
    
    validFromText: string;
    lastUsedText: KnockoutComputed<string>;
}

class certificates extends viewModelBase {

    view = require("views/manage/certificates.html");

    spinners = {
        processing: ko.observable<boolean>(false),
        lastUsed: ko.observable<boolean>(true)
    };
    
    model = ko.observable<certificateModel>();
    showDatabasesSelector: KnockoutComputed<boolean>;
    canExportServerCertificates: KnockoutComputed<boolean>;
    canReplaceServerCertificate: KnockoutComputed<boolean>;
    certificates = ko.observableArray<unifiedCertificateDefinition>();
    
    serverCertificateThumbprint = ko.observable<string>();
    clientCertificateThumbprint = ko.observable<string>();
    
    serverCertificateSetupMode = ko.observable<Raven.Server.Commercial.SetupMode>();
    serverCertificateRenewalDate = ko.observable<string>();
    
    wellKnownAdminCerts = ko.observableArray<string>([]);
    wellKnownAdminCertsVisible = ko.observable<boolean>(false);
    
    wellKnownIssuers = ko.observableArray<string>([]);
    wellKnownIssuersVisible = ko.observable<boolean>(false);
    
    domainsForServerCertificate = ko.observableArray<string>([]);
    
    isSecureServer = accessManager.default.secureServer();
    
    accessManager = accessManager.default;

    importedFileName = ko.observable<string>();
    
    newPermissionDatabaseName = ko.observable<string>();
    
    generateCertificateUrl = endpoints.global.adminCertificates.adminCertificates;
    exportCertificateUrl = endpoints.global.adminCertificates.adminCertificatesExport;
    generateCertPayload = ko.observable<string>();

    clearanceLabelFor = certificateModel.clearanceLabelFor;
    securityClearanceTypes = certificateModel.securityClearanceTypes;
    
    nameFilter = ko.observable<string>("");
    
    showAdminCertificates = ko.observable<boolean>(true);
    showOperatorCertificates = ko.observable<boolean>(true);
    showUserCertificates = ko.observable<boolean>(true);

    showValidCertificates = ko.observable<boolean>(true);
    showExpiredCertificates = ko.observable<boolean>(false);
    showAboutToExpireCertificates = ko.observable<boolean>(true);
    
    databases = databasesManager.default.databases;
    
    inputDatabase = ko.observable<string>();
    databasesToShow = ko.observableArray<string>([]); // empty means show all, no specific dbs are selected
    allDatabasesSelected: KnockoutComputed<boolean>;

    hasAnyFilters: KnockoutComputed<boolean>;
    filterAndSortDescription: KnockoutComputed<string>;
    noCertificateIsVisible: KnockoutComputed<boolean>;
    
    currentSortMode = ko.observable<certificatesSortMode>("default");
    sortModeText: KnockoutComputed<string>;

    deleteExistingCertificate = ko.observable<boolean>(false);

    // currently displayed QR Code
    private qrCode: any;
    
    hasReadOnlyCertificates = licenseModel.getStatusValue("HasReadOnlyCertificates");
    infoHubView: ReactInKnockout<typeof CertificatesInfoHub>;
    
    constructor() {
        super();

        this.bindToCurrentInstance("onCloseEdit", "save", "enterEditCertificateMode", "enterRegenerateCertificateMode",
            "deletePermission", "fileSelected", "copyThumbprint",
            "deleteCertificateConfirm", "renewServerCertificate", "canBeAutomaticallyRenewed",
            "sortCertificates", "clearAllFilters", "syncQrCode", 
            "addPermission","addPermissionWithBlink","addDatabase","addDatabaseWithBlink");
        
        this.initObservables();
        
        this.nameFilter.throttle(300).subscribe(() => this.filterCertificates());
        
        this.showValidCertificates.subscribe(() => this.filterCertificates());
        this.showExpiredCertificates.subscribe(() => this.filterCertificates());
        this.showAboutToExpireCertificates.subscribe(() => this.filterCertificates());
        
        this.showAdminCertificates.subscribe(() => this.filterCertificates());
        this.showOperatorCertificates.subscribe(() => this.filterCertificates());
        this.showUserCertificates.subscribe(() => this.filterCertificates());
        
        this.databasesToShow.subscribe(() => this.filterCertificates());

        this.infoHubView = ko.pureComputed(() => ({
            component: CertificatesInfoHub
        }));
    }
    
    activate() {
       this.loadCertificatesAndLastUsedDates();
        
        if (accessManager.default.isClusterAdminOrClusterNode()) {
            new getServerCertificateSetupModeCommand()
                .execute()
                .done((setupMode: Raven.Server.Commercial.SetupMode) => {
                    this.serverCertificateSetupMode(setupMode);

                    if (setupMode === "LetsEncrypt") {
                        this.fetchRenewalDate();
                    }
                });
        }
    }
    
    compositionComplete() {
        super.compositionComplete();
        
        this.addNotification(changesContext.default.serverNotifications().watchAllAlerts(alert => this.onAlert(alert)));

        $('.filter-options-dropdown-container [data-toggle="tooltip"]').tooltip();
        
        $(".js-export-certificates").tooltip({
            container: "body",
            placement: "right",
            html: true,
            title: `<div class="text-left">Export the server certificate(s) without their private key into a .pfx file.
                   These certificates can be used during a manual cluster setup, when you need to register server certificates to be trusted on other nodes.</div>`
        });
        
        this.model.subscribe(model => {
            if (!model || !model.requireTwoFactor()) {
                if (this.qrCode) {
                    this.qrCode.clear();
                    this.qrCode = null;
                }
            }
            
            if (model) {
                this.initPopover();
                
                model.name.subscribe(() => this.syncQrCode());
                model.authenticationKey.subscribe(() => this.syncQrCode());
                model.requireTwoFactor.subscribe(twoFactor => {
                    if (twoFactor) {
                        this.generateSecret(model);
                    }
                });
            }
        });
    }
    
    syncQrCode(): void {
        if (!this.model().requireTwoFactor() || !this.model().authenticationKey()) {
            return;
        }
        
        const secret = this.model().authenticationKey();
        const encodedIssuer = encodeURIComponent(location.hostname);
        const encodedName = encodeURIComponent(this.model().name() ?? "Client Certificate");
        
        const uri = `otpauth://totp/${encodedIssuer}:${encodedName}?secret=${secret}&issuer=${encodedIssuer}`;
        
        const qrContainer = document.getElementById("encryption_qrcode");

        if (qrContainer.innerHTML && !this.qrCode) {
            // clean up old instances
            qrContainer.innerHTML = "";
        }

        if (!this.qrCode) {
            this.qrCode = new QRCode(qrContainer, {
                text: uri,
                width: 256,
                height: 256,
                colorDark: "#000000",
                colorLight: "#ffffff",
                correctLevel: QRCode.CorrectLevel.Q
            });
        } else {
            this.qrCode.clear();
            this.qrCode.makeCode(uri);
        }
    }
    
    private generateSecret(model: certificateModel): void {
        new generateTwoFactorSecretCommand()
            .execute()
            .done(key => {
                model.authenticationKey(key.Secret);
            });
    }
    
    private async loadCertificateLastUsedDates(): Promise<Record<string, string>> {
        this.spinners.lastUsed(true);
        try {
            const nodeTags = clusterTopologyManager.default.topology().nodes().map(x => x.tag());
            const tasks = nodeTags.map(async tag => {
                try {
                    const stats = await new getAdminStatsCommand(tag).execute();
                    return stats.LastRequestTimePerCertificate;
                } catch (e) {
                    // we ignore errors here
                    return {};
                }
            });

            const allStats = await Promise.all(tasks);
            
            const result: Record<string, string> = {};
            allStats.forEach(nodeStats => {
                Object.keys(nodeStats).forEach(thumbprint => {
                    const lastUsed = nodeStats[thumbprint];
                    
                    if (!result[thumbprint] || result[thumbprint].localeCompare(lastUsed) > 0) {
                        result[thumbprint] = lastUsed;
                    }
                })
            });
            
            return result;
        } finally {
            this.spinners.lastUsed(false);    
        } 
    }
    
    private filterCertificates(): void {
        this.certificates().forEach((certificate: unifiedCertificateDefinitionWithCache) => {
            certificate.Visible(this.isMatchingTextFilter(certificate) &&
                this.isMatchingClearance(certificate) &&
                this.isMatchingState(certificate) &&
                this.isMatchingDatabase(certificate));
        });
        
        const wellKnownAdminCerts = this.wellKnownAdminCerts();
        const nameLower = this.nameFilter().toLocaleLowerCase();
        
        if (wellKnownAdminCerts.length && this.showAdminCertificates()) {
            const thumbprintMatch = _.some(wellKnownAdminCerts, x => x.toLocaleLowerCase().includes(nameLower));
            this.wellKnownAdminCertsVisible(thumbprintMatch);
        } else {
            this.wellKnownAdminCertsVisible(false);
        }

        const wellKnownIssues = this.wellKnownIssuers();

        if (wellKnownIssues.length && this.showAdminCertificates()) {
            const thumbprintMatch = _.some(wellKnownIssues, x => x.toLocaleLowerCase().includes(nameLower));
            this.wellKnownIssuersVisible(thumbprintMatch);
        } else {
            this.wellKnownIssuersVisible(false);
    }
    }
    
    private isMatchingTextFilter(certificate: unifiedCertificateDefinitionWithCache): boolean {
        const textFilter = this.nameFilter().toLocaleLowerCase();
        
        const nameMatch = certificate.Name.toLocaleLowerCase().includes(textFilter);
        const thumbprintMatch = _.some(certificate.Thumbprints, x => x.toLocaleLowerCase().includes(textFilter));
        
        return nameMatch || thumbprintMatch;
    }
    
    private isMatchingClearance(certificate: unifiedCertificateDefinitionWithCache): boolean {
        const clearance = certificate.SecurityClearance;
        
        const adminMatch = this.showAdminCertificates() && (clearance === "ClusterAdmin" || clearance === "ClusterNode");
        const operatorMatch = this.showOperatorCertificates() && clearance === "Operator";
        const userMatch = this.showUserCertificates() && clearance === "ValidUser";
        
        return adminMatch || operatorMatch || userMatch;
    }

    private isMatchingState(certificate: unifiedCertificateDefinitionWithCache): boolean {
        const state = certificate.expirationClass;
        
        const validMatch = this.showValidCertificates() && !state;
        const expiredMatch = this.showExpiredCertificates() && state === "text-danger";
        const aboutToExpireMatch = this.showAboutToExpireCertificates() && state === "text-warning";
        
        return validMatch || expiredMatch || aboutToExpireMatch;
    }
    
    private isMatchingDatabase(certificate: unifiedCertificateDefinitionWithCache): boolean {
        const clearance = certificate.SecurityClearance;
        
        const dbToShow = this.databasesToShow();
        const dbInCertificate = Object.keys(certificate.Permissions);
        const found = dbToShow.some(dbItem => dbInCertificate.indexOf(dbItem) >= 0);

        return this.allDatabasesSelected() || found ||
            clearance === "Operator" || clearance === "ClusterAdmin" || clearance === "ClusterNode";
    }
    
    clearAllFilters(): void {
        this.nameFilter("");

        this.showAdminCertificates(true);
        this.showOperatorCertificates(true);
        this.showUserCertificates(true);
        
        this.showValidCertificates(true);
        this.showExpiredCertificates(true);
        this.showAboutToExpireCertificates(true);
        
        this.databasesToShow([]);
    }
    
    private onAlert(alert: Raven.Server.NotificationCenter.Notifications.AlertRaised): void {
        if (alert.AlertType === "Certificates_ReplaceError" ||
            alert.AlertType === "Certificates_ReplaceSuccess" ||
            alert.AlertType === "Certificates_EntireClusterReplaceSuccess") {
            this.loadCertificatesAndLastUsedDates();
        }
    }
    
    private initPopover(): void {
        popoverUtils.longWithHover($(".certificate-file-label small"),
            {
                content: () => {
                    switch (this.model().mode()) {
                        case "replace":
                            return `<small>Certificate file cannot be password protected.</small>`;
                        case "upload":
                            return `<ul class="margin-top margin-top-xs padding padding-xs margin-left margin-bottom margin-bottom-xs">
                                        <li><small>Select a <strong>.pfx file</strong> with single or multiple certificates.</small></li>
                                        <li><small>All certificates will be imported under a single name.</small></li>
                                    </ul>`;
                    }
                },
                html: true,
                placement: "top"
            });
        $('.certificates [data-toggle="tooltip"]').tooltip();
    }
    
    private initObservables() {
        this.showDatabasesSelector = ko.pureComputed(() => {
            if (!this.model()) {
                return false;
            }
            
            return this.model().securityClearance() === "ValidUser";
        });
        
        this.canExportServerCertificates = ko.pureComputed(() => {
            const certs = this.certificates();
            return _.some(certs, x => x.SecurityClearance === "ClusterNode");
        });
        
        this.canReplaceServerCertificate = ko.pureComputed(() => {
            const certs = this.certificates();
            return _.some(certs, x => x.SecurityClearance === "ClusterNode");
        });

        this.allDatabasesSelected = ko.pureComputed(() => !this.databasesToShow().length);

        this.hasAnyFilters = ko.pureComputed(() => !!this.filterAndSortDescription() || !!this.nameFilter());
        
        this.filterAndSortDescription = ko.pureComputed(() => {
            const clearanceItems: string[] = [];
            if (this.showAdminCertificates()) { clearanceItems.push("Admin clearance"); }
            if (this.showOperatorCertificates()) { clearanceItems.push("Operator clearance"); }
            if (this.showUserCertificates()) { clearanceItems.push("User clearance"); }

            if (!clearanceItems.length) {
                return `<div class="bg-warning text-warning padding padding-xs">
                            <i class="icon-warning"></i><span>No security clearance is selected in Filter Options</span>
                        </div>`;
    }
    
            const stateItems: string[] = [];
            if (this.showValidCertificates()) { stateItems.push("Valid"); }
            if (this.showAboutToExpireCertificates()) { stateItems.push("About to expire"); }
            if (this.showExpiredCertificates()) { stateItems.push("Expired"); }

            if (!stateItems.length) {
                return `<div class="bg-warning text-warning padding padding-xs">
                            <i class="icon-warning"></i><span>No state is selected in Filter Options</span>
                        </div>`;
            }

            const clearancePart = clearanceItems.length === 3 ? "" : `<strong>Clearance</strong>: ${clearanceItems.join(", ")}`;
            const statePart = stateItems.length === 3 ? "" : `<strong>State</strong>: ${stateItems.join(", ")}`;

            const databases = this.databasesToShow().join(", ");
            const databasesPart = databases.length ? `<strong>Databases</strong>: ${databases}` : "";
            
            const sortPart = this.sortModeText() ? `<strong>Sorted by</strong>: ${this.sortModeText()} <br />` : "";
            
            return `${sortPart}${clearancePart}${clearancePart ? "<span class='margin-right-sm'></span>" : ""}${statePart}${clearancePart || statePart ? "<br />" : ""}${databasesPart}`;
        });
        
        this.sortModeText = ko.pureComputed(() => {
            const mode = this.currentSortMode();
            
            switch (mode) {
                case "byNameAsc":
                    return "Name - Ascending";
                case "byNameDesc":
                    return "Name - Descending";
                case "byExpirationAsc":
                    return "Expiration Date - Ascending";
                case "byExpirationDesc":
                    return "Expiration Date - Descending";
                case "byValidFromAsc":
                    return "Valid-From Date - Ascending";
                case "byValidFromDesc":
                    return "Valid-From Date - Descending";
                case "byLastUsedAsc":
                    return "Last Used Date - Ascending";
                case "byLastUsedDesc":
                    return "Last Used Date - Descending";
                case "default":
                    return "";
                default:
                    assertUnreachable(mode);
            }
        });
        
        this.noCertificateIsVisible = ko.pureComputed(() => {
           const found = this.certificates().find(x => x.Visible());
           return !found; 
        });
    }
    
    private fetchRenewalDate() {
        return new getServerCertificateRenewalDateCommand()
            .execute()
            .done(dateAsString => {
                const date = moment.utc(dateAsString);
                const dateFormatted = date.format("YYYY-MM-DD");
                this.serverCertificateRenewalDate(dateFormatted);
            })
    }
    
    databasesAccessInfo(certificateDefinition: Raven.Client.ServerWide.Operations.Certificates.CertificateDefinition) {
        return certificateModel.resolveDatabasesAccess(certificateDefinition);
    }

    getAccessIcon(accessLevel: databaseAccessLevel) {
        return accessManager.default.getAccessIcon(accessLevel);
    }

    getAccessColor(accessLevel: databaseAccessLevel) {
        return accessManager.default.getAccessColor(accessLevel);
    }

    getAccessInfoText(accessLevel: databaseAccessLevel) {
        return accessManager.default.getAccessLevelText(accessLevel);
    }
    
    canBeAutomaticallyRenewed(thumbprints: string[]) {
        return ko.pureComputed(() => {
            return _.includes(thumbprints, this.serverCertificateThumbprint()) && this.serverCertificateSetupMode() === 'LetsEncrypt';
        });
    }
    
    renewServerCertificate() {
        this.confirmationMessage("Server certificate renewal", "Do you want to renew the server certificate?", {
            buttons: ["No", "Yes, renew"]
        })
            .done(result => {
                if (result.can) {
                    new forceRenewServerCertificateCommand()
                        .execute();
                }
            });
    }
    
    enterEditCertificateMode(itemToEdit: unifiedCertificateDefinition) {
        if (!_.includes(itemToEdit.Thumbprints, this.serverCertificateThumbprint())) {
            this.model(certificateModel.fromDto(itemToEdit));
            this.model().validationGroup.errors.showAllMessages(false);
        }
    }

    deleteCertificateConfirm(certificate: Raven.Client.ServerWide.Operations.Certificates.CertificateDefinition) {
        this.confirmationMessage("Are you sure?", "Do you want to delete certificate with thumbprint: " + generalUtils.escapeHtml(certificate.Thumbprint) + "", {
            buttons: ["No", "Yes, delete"],
            html: true
        })
            .done(result => {
                if (result.can) {
                    this.deleteCertificate(certificate.Thumbprint);
                }
            });
    }
    
    private deleteCertificate(thumbprint: string) {
        eventsCollector.default.reportEvent("certificates", "delete");
        
        new deleteCertificateCommand(thumbprint)
            .execute()
            .always(() => this.loadCertificatesAndLastUsedDates());
    }

    exportServerCertificates() {
        eventsCollector.default.reportEvent("certificates", "export-certs");
        const targetFrame = $("form#certificates_download_form");
        targetFrame.attr("action", this.exportCertificateUrl);
        targetFrame.submit();
    }
    
    enterGenerateCertificateMode() {
        eventsCollector.default.reportEvent("certificates", "generate");
        this.model(certificateModel.generate());
    }

    enterRegenerateCertificateMode(itemToRegenerate: unifiedCertificateDefinition) {
        eventsCollector.default.reportEvent("certificates", "re-generate");
        this.model(certificateModel.regenerate(itemToRegenerate));
    }
    
    enterUploadCertificateMode() {
        eventsCollector.default.reportEvent("certificates", "upload");
        this.model(certificateModel.upload());
    }
    
    replaceServerCertificate() {
        eventsCollector.default.reportEvent("certificates", "replace");
        this.model(certificateModel.replace());
        
        new getClusterDomainsCommand()
            .execute()
            .done(domains => this.domainsForServerCertificate(domains));
    }

    fileSelected(fileInput: HTMLInputElement) {
        fileImporter.readAsDataURL(fileInput, (dataUrl, fileName) => {
            const isFileSelected = fileName ? !!fileName.trim() : false;
            this.importedFileName(isFileSelected ? fileName.split(/(\\|\/)/g).pop() : null);

            // dataUrl has following format: data:;base64,PD94bW... trim on first comma
            this.model().certificateAsBase64(dataUrl.substr(dataUrl.indexOf(",") + 1));
        });
    }

    save() {
        const model = this.model();
        const thumbprint = model.thumbprint();
        
        if (!this.isValid(model.validationGroup)) {
            return;
        }

        const maybeWarnTask = $.Deferred<void>();
        
        if (model.mode() !== "replace" && model.securityClearance() === "ValidUser" && model.permissions().length === 0) {
            this.confirmationMessage("Did you forget about assigning database privileges?",
            "Leaving the database privileges section empty is going to prevent users from accessing the database.",
                {
                    buttons: ["I want to assign privileges", "Save anyway"],
                    forceRejectWithResolve: true
                })
            .done(result => {
                if (result.can) {
                    maybeWarnTask.resolve();
                }
            });
        } else {
            maybeWarnTask.resolve();
        }

        maybeWarnTask
            .done(() => {
                this.spinners.processing(true);

                switch (model.mode()) {
                    case "generate":
                    case "regenerate":
                        this.generateCertPayload(JSON.stringify(model.toGenerateCertificateDto()));

                        new getNextOperationIdCommand(null)
                            .execute()
                            .done(operationId => {

                                const targetFrame = $("form#certificate_download_form");
                                targetFrame.attr("action", this.generateCertificateUrl + "?operationId=" + operationId + "&raft-request-id=" + generalUtils.generateUUID());
                                targetFrame.submit();

                                notificationCenter.instance.monitorOperation(null, operationId)
                                    .done(() => {
                                        if (model.deleteExpired()) {
                                            this.deleteCertificate(thumbprint);
                                        }
                                        messagePublisher.reportSuccess("Client certificate was generated and downloaded successfully.");
                                    })
                                    .fail(() => {
                                        notificationCenter.instance.openDetailsForOperationById(null, operationId);
                                    })
                                    .always(() => {
                                        this.spinners.processing(false);
                                        this.onCloseEdit();
                                        this.loadCertificatesAndLastUsedDates();
                                    })
                            })
                            .fail(() => {
                                this.spinners.processing(false);
                                this.onCloseEdit();
                            });
                        break;
                        
                    case "upload":
                        new uploadCertificateCommand(model)
                            .execute()
                            .always(() => {
                                this.spinners.processing(false);
                                this.loadCertificatesAndLastUsedDates();
                                this.onCloseEdit();
                            });
                        break;

                    case "editExisting":
                        new updateCertificatePermissionsCommand(model)
                            .execute()
                            .always(() => {
                                this.spinners.processing(false);
                                this.loadCertificatesAndLastUsedDates();
                                this.onCloseEdit();
                            });
                        break;
                        
                    case "replace":
                        new replaceClusterCertificateCommand(model)
                            .execute()
                            .always(() => {
                                this.spinners.processing(false);
                                this.loadCertificatesAndLastUsedDates();
                                this.onCloseEdit();
                            });
                }
            });
    }
    
    private loadCertificatesAndLastUsedDates() {
        this.loadCertificates()
            .done(() => {
                this.loadCertificateLastUsedDates()
                    .then(lastUsed => {
                        Object.keys(lastUsed).map(thumbprint => {
                            const lastUsedDate = lastUsed[thumbprint];

                            const certificateToApply = this.certificates().find(x => x.Thumbprint === thumbprint);
                            if (certificateToApply) {
                                certificateToApply.LastUsed(lastUsedDate);
                            }
                        });
                    });
            });
    }
    
    private loadCertificates() {
        return new getCertificatesCommand(true)
            .execute()
            .done(certificatesInfo => {
                const mergedCertificates: unifiedCertificateDefinition[] = [];
                
                const secondaryCertificates: Raven.Client.ServerWide.Operations.Certificates.CertificateDefinition[] = [];
                
                certificatesInfo.Certificates.forEach(cert => {
                    cert.HasTwoFactor = cert.HasTwoFactor ?? false; // force property to exist
                    
                    if (cert.CollectionPrimaryKey) {
                        secondaryCertificates.push(cert);
                    } else {
                        (cert as unifiedCertificateDefinition).Thumbprints = [cert.Thumbprint];
                        (cert as unifiedCertificateDefinition).Visible = ko.observable<boolean>(true);
                        (cert as unifiedCertificateDefinition).LastUsed = ko.observable<string>();
                        mergedCertificates.push(cert as unifiedCertificateDefinition);
                    }
                });
                
                this.serverCertificateThumbprint(certificatesInfo.LoadedServerCert);
                this.clientCertificateThumbprint(accessManager.clientCertificateThumbprint());
                
                secondaryCertificates.forEach(cert => {
                    const thumbprint = cert.CollectionPrimaryKey;
                    const primaryCert = mergedCertificates.find(x => x.Thumbprint === thumbprint);
                    
                    if (primaryCert) {
                        primaryCert.Thumbprints.push(cert.Thumbprint);
                    }
                });
                
                const orderedCertificates = this.sortByDefaultInternal(mergedCertificates);
                
                this.updateCache(orderedCertificates);
                this.certificates(orderedCertificates);
                
                this.wellKnownAdminCerts(certificatesInfo.WellKnownAdminCerts || []);
                this.wellKnownIssuers(certificatesInfo.WellKnownIssuers || []);
                this.filterCertificates();
                this.sortCertificates(this.currentSortMode());
            });
    }
    
    private updateCache(certificates: Array<unifiedCertificateDefinition>): void {
        certificates.forEach((cert: unifiedCertificateDefinitionWithCache) => {
            const expirationDate = moment.utc(cert.NotAfter);
            const expirationDateFormatted = expirationDate.format("YYYY-MM-DD");
            
            const nowPlusExpirationThresholdInDays = moment.utc().add(serverSettings.default.certificateExpiringThresholdInDays(), 'days');
            cert.isExpired = false;
            cert.isAboutToExpire = false;
            
            if (expirationDate.isBefore()) {
                cert.expirationText = 'Expired ' + expirationDateFormatted;
                cert.expirationIcon = "icon-danger";
                cert.expirationClass = "text-danger";
                cert.isExpired = true;
            } else if (expirationDate.isAfter(nowPlusExpirationThresholdInDays)) {
                cert.expirationText = expirationDateFormatted;
                cert.expirationIcon = "icon-expiration"; 
                cert.expirationClass = "";
            } else { 
                cert.expirationText = expirationDateFormatted;
                cert.expirationIcon = "icon-warning";
                cert.expirationClass = "text-warning";
                cert.isAboutToExpire = true;
            }

            if (cert.NotBefore) {
                const validFromDate = moment.utc(cert.NotBefore);
                cert.validFromText = validFromDate.format("YYYY-MM-DD");
            } else {
                cert.validFromText = "Unavailable";
            }
            
            cert.lastUsedText = ko.pureComputed(() => {
                const lastUsed = cert.LastUsed();
                if (!lastUsed) {
                    return "(not used)";
                }
                
                return moment.utc(lastUsed).format("YYYY-MM-DD");
            });
        });
    }
    
    onCloseEdit() {
        this.model(null);
    }
    
    deletePermission(permission: certificatePermissionModel): void {
        const model = this.model();
        model.permissions.remove(permission);
    }

    addPermission(): void {
        const databaseNameToUse = this.newPermissionDatabaseName();
    
        if (!this.model().permissions().find(x => x.databaseName() === databaseNameToUse)) {
            this.addPermissionWithBlink(databaseNameToUse);
        } else {
            this.newPermissionDatabaseName("");
        }
    }
        
    addPermissionWithBlink(databaseName: string): void {
        const permission = new certificatePermissionModel();
        permission.databaseName(databaseName);
        permission.accessLevel("ReadWrite");
        
        this.model().permissions.unshift(permission);
        $(".generate-certificate .collection-list li").first().addClass("blink-style");
        
        this.newPermissionDatabaseName("");
    }

    createDatabaseNameAutoCompleter() {
        return ko.pureComputed(() => {
            const key = this.newPermissionDatabaseName();
            
            const existingPermissions = this.model().permissions().map(x => x.databaseName());
            
            const dbNames = databasesManager.default.databases()
                .map(x => x.name)
                .filter(x => !_.includes(existingPermissions, x));

            if (key) {
                return dbNames.filter(x => x.toLowerCase().includes(key.toLowerCase()));
            } else {
                return dbNames;
            }
        });
    }
    
    copyThumbprint(thumbprint: string): void {
        copyToClipboard.copy(thumbprint, "Thumbprint was copied to clipboard.");
    }

    copyAuthenticationKeyToClipboard(authenticationKey: string) {
        copyToClipboard.copy(authenticationKey, "Authentication Key was copied to clipboard.");
    }
    
    canEdit(model: unifiedCertificateDefinition) {
        return ko.pureComputed(() => {
            if (_.includes(model.Thumbprints, this.serverCertificateThumbprint())) {
                return false;
            }

            return true;
        });
    }
    
    canDelete(securityClearance: Raven.Client.ServerWide.Operations.Certificates.SecurityClearance, model: unifiedCertificateDefinition) {
        return ko.pureComputed(() => {
            if (!this.accessManager.isClusterAdminOrClusterNode() && securityClearance === "ClusterAdmin") {
                return false;
            }
            
            if (!this.accessManager.isClusterAdminOrClusterNode() && securityClearance === "ClusterNode") {
                return false;
            }
            
            if (_.includes(model.Thumbprints, this.serverCertificateThumbprint())) {
                return false;
            }
            
            return true; 
        });
    }

    canGenerateCertificateForSecurityClearanceType(securityClearance: Raven.Client.ServerWide.Operations.Certificates.SecurityClearance) {
        return ko.pureComputed(() => {
            if (!this.accessManager.isClusterAdminOrClusterNode() && securityClearance === "ClusterAdmin") {
                return false;
            }

            return true;
        });
    }
    
    sortByDefault(): void {
        const orderedCertificates = this.sortByDefaultInternal(this.certificates());
        this.certificates(orderedCertificates);
}

    sortByDefaultInternal(certList: unifiedCertificateDefinition[]): unifiedCertificateDefinition[] {
        let orderedCertificates = certList;
        
        orderedCertificates.sort((a, b) =>
            generalUtils.sortAlphaNumeric(a.Name.toLocaleLowerCase(), b.Name.toLocaleLowerCase(), "asc"));
        
        const serverCert = certList.find(x => _.includes(x.Thumbprints, this.serverCertificateThumbprint()));
        const clientCert = certList.find(x => _.includes(x.Thumbprints, this.clientCertificateThumbprint()));
        orderedCertificates = certList.filter(x => x !== serverCert && x !== clientCert);

        if (clientCert && serverCert && clientCert.Thumbprint !== serverCert.Thumbprint) {
            orderedCertificates.unshift(clientCert);
        }
        
        orderedCertificates.unshift(serverCert);
        return orderedCertificates
    }
    
    sortCertificates(mode: certificatesSortMode) {
        this.currentSortMode(mode);
        
        switch (mode) {
            case "byNameAsc":
                this.sortByName("asc");
                break;
            case "byNameDesc":
                this.sortByName("desc");
                break;
            case "byExpirationAsc":
                this.sortByExpiration("asc");
                break;
            case "byExpirationDesc":
                this.sortByExpiration("desc");
                break;
            case "byLastUsedAsc":
                this.sortByLastUsed("asc");
                break;
            case "byLastUsedDesc":
                this.sortByLastUsed("desc");
                break;
            case "byValidFromAsc":
                this.sortByValidFrom("asc");
                break;
            case "byValidFromDesc":
                this.sortByValidFrom("desc");
                break;
            case "default":
                this.sortByDefault();
                break;
        }
    }

    private sortByName(mode: sortMode): void {
        this.certificates.sort((a, b) =>
            generalUtils.sortAlphaNumeric(a.Name.toLocaleLowerCase(), b.Name.toLocaleLowerCase(), mode as sortMode));
    }

    private sortByExpiration(mode: sortMode): void {
        this.certificates.sort((a, b) => {
            const sgn = mode === "asc" ? 1 : -1;
            return sgn * a.NotAfter.localeCompare(b.NotAfter);
        });
    }

    private sortByLastUsed(mode: sortMode): void {
        this.certificates.sort((a, b) => {
            const sgn = mode === "asc" ? 1 : -1;
            const firstLastUsed = a.LastUsed() ?? "n/a";
            const secondLastUsed = b.LastUsed() ?? "n/a";
            return sgn * firstLastUsed.localeCompare(secondLastUsed);
        });
    }

    private sortByValidFrom(mode: sortMode): void {
        this.certificates.sort((a, b) => {
            const sgn = mode === "asc" ? 1 : -1;
            return sgn * a.NotBefore.localeCompare(b.NotBefore);
        });
    }

    createDatabaseNameAutoCompleterForFilter() {
        return ko.pureComputed(() => {
            const key = this.inputDatabase();

            const options = this.databases().map(x => x.name);

            const usedOptions = this.databasesToShow().filter(k => k !== key);

            const filteredOptions: string[] = _.difference(options, usedOptions);

            if (key) {
                return filteredOptions.filter(x => x.toLowerCase().includes(key.toLowerCase()));
            } else {
                return filteredOptions;
            }
        });
    }

    addDatabase(): void {
        const databaseToAdd = this.inputDatabase();
        
        if (!this.databasesToShow().find(x => x === databaseToAdd)) {
            this.addDatabaseWithBlink(databaseToAdd);
        } else {
            this.inputDatabase("");
        }
    }

    removeDatabase(database: string): void {
        this.databasesToShow.remove(database);
    }

    addDatabaseWithBlink(databaseName: string): void {
        this.databasesToShow.unshift(databaseName);
        
        $(".filter-options-dropdown-container .collection-list li").first().addClass("blink-style");
        
        this.inputDatabase("");
    }
}

export = certificates;
