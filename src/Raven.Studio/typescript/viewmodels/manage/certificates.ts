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
import getNextOperationId = require("commands/database/studio/getNextOperationId");
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

interface unifiedCertificateDefinitionWithCache extends unifiedCertificateDefinition {
    expirationClass: string;
    expirationText: string;
    expirationIcon: string;
    validFromClass: string;
    validFromText: string;
    validFromIcon: string;
}

class certificates extends viewModelBase {

    spinners = {
        processing: ko.observable<boolean>(false)
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
    
    domainsForServerCertificate = ko.observableArray<string>([]);
    
    isSecureServer = accessManager.default.secureServer();
    
    accessManager = accessManager.default.certificatesView;

    importedFileName = ko.observable<string>();
    
    newPermissionDatabaseName = ko.observable<string>();
    canAddPermission: KnockoutComputed<boolean>;
    
    newPermissionValidationGroup: KnockoutValidationGroup = ko.validatedObservable({
        newPermissionDatabaseName: this.newPermissionDatabaseName
    });

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
    showExpiredCertificates = ko.observable<boolean>(true);
    showAboutToExpireCertificates = ko.observable<boolean>(true);
    
    databases = databasesManager.default.databases;
    
    inputDatabase = ko.observable<string>();
    databasesToShow = ko.observableArray<string>([]); // empty means show all, no specific dbs are selected

    canAddDatabase: KnockoutComputed<boolean>;
    allDatabasesSelected: KnockoutComputed<boolean>;

    hasAnyFilters: KnockoutComputed<boolean>;
    filterAndSortDescription: KnockoutComputed<string>;
    sortCriteria = ko.observable<string>();
    noCertificateIsVisible: KnockoutComputed<boolean>;
    
    constructor() {
        super();

        this.bindToCurrentInstance("onCloseEdit", "save", "enterEditCertificateMode", 
            "deletePermission", "addNewPermission", "fileSelected", "copyThumbprint",
            "useDatabase", "deleteCertificate", "renewServerCertificate", "canBeAutomaticallyRenewed",
            "sortByDefault", "sortByName", "sortByExpiration", "sortByValidFrom", "clearAllFilters");
        
        this.initObservables();
        this.initValidation();
        
        this.nameFilter.throttle(300).subscribe(() => this.filterCertificates());
        
        this.showValidCertificates.subscribe(() => this.filterCertificates());
        this.showExpiredCertificates.subscribe(() => this.filterCertificates());
        this.showAboutToExpireCertificates.subscribe(() => this.filterCertificates());
        
        this.showAdminCertificates.subscribe(() => this.filterCertificates());
        this.showOperatorCertificates.subscribe(() => this.filterCertificates());
        this.showUserCertificates.subscribe(() => this.filterCertificates());
        
        this.databasesToShow.subscribe(() => this.filterCertificates());
    }
    
    activate() {
        this.loadCertificates();
        
        if (accessManager.default.certificatesView.canRenewLetsEncryptCertificate()) {
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
            if (model) {
                this.initPopover();
            }
        });
    }
    
    private filterCertificates() {
        const filter = this.nameFilter().toLocaleLowerCase();
        
        this.certificates().forEach((certificate: unifiedCertificateDefinitionWithCache) => {
            
            const nameMatch = !certificate || certificate.Name.toLocaleLowerCase().includes(filter);
            const thumbprintMatch = !certificate || _.some(certificate.Thumbprints, x => x.toLocaleLowerCase().includes(filter));

            const validMatch = !certificate || (this.showValidCertificates() && !certificate.expirationClass);
            const expiredMatch = !certificate || (this.showExpiredCertificates() && certificate.expirationClass === "text-danger");
            const aboutToExpireMatch = !certificate || (this.showAboutToExpireCertificates() && certificate.expirationClass === "text-warning");

            const adminMatch = !certificate || (this.showAdminCertificates() && (certificate.SecurityClearance === "ClusterAdmin" || certificate.SecurityClearance === "ClusterNode"));
            const operatorMatch = !certificate || (this.showOperatorCertificates() && certificate.SecurityClearance === "Operator");
            const userMatch = !certificate || (this.showUserCertificates() && certificate.SecurityClearance === "ValidUser");
            
            const dbToShow = this.databasesToShow();
            const dbInCertificate = Object.keys(certificate.Permissions);
            const found = dbToShow.some(dbItem => dbInCertificate.indexOf(dbItem) >= 0);
            
            const dbMatch = !certificate || this.allDatabasesSelected() || found ||
                certificate.SecurityClearance === "Operator" || certificate.SecurityClearance === "ClusterAdmin" || certificate.SecurityClearance === "ClusterNode";
            
            certificate.Visible((nameMatch || thumbprintMatch) &&
                (validMatch || expiredMatch || aboutToExpireMatch) &&
                (adminMatch || operatorMatch || userMatch) &&
                 dbMatch);
        });
        
        const wellKnownAdminCerts = this.wellKnownAdminCerts();
        
        if (wellKnownAdminCerts.length && this.showAdminCertificates()) {
            const thumbprintMatch = _.some(wellKnownAdminCerts, x => x.toLocaleLowerCase().includes(filter));
            this.wellKnownAdminCertsVisible(thumbprintMatch);
        } else {
            this.wellKnownAdminCertsVisible(false);
        }
    }
    
    clearAllFilters() {
        this.nameFilter("");

        this.showAdminCertificates(true);
        this.showOperatorCertificates(true);
        this.showUserCertificates(true);
        
        this.showValidCertificates(true);
        this.showExpiredCertificates(true);
        this.showAboutToExpireCertificates(true);
        
        this.databasesToShow([]);
    }
    
    private onAlert(alert: Raven.Server.NotificationCenter.Notifications.AlertRaised) {
        if (alert.AlertType === "Certificates_ReplaceError" ||
            alert.AlertType === "Certificates_ReplaceSuccess" ||
            alert.AlertType === "Certificates_EntireClusterReplaceSuccess") {
            this.loadCertificates();
        }
    }
    
    private initPopover() {
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

        this.canAddPermission = ko.pureComputed(() => {
            const databasePermissionToAdd = this.newPermissionDatabaseName();
            return databasePermissionToAdd && !this.model().permissions().find(x => x.databaseName() === databasePermissionToAdd);
        });
        
        this.canAddDatabase = ko.pureComputed(() => {
            const databaseToAdd = this.inputDatabase();
            return databaseToAdd && !this.databasesToShow().find(x => x === databaseToAdd);
        });

        this.allDatabasesSelected = ko.pureComputed(() => !this.databasesToShow().length);

        this.hasAnyFilters = ko.pureComputed(() => !!this.filterAndSortDescription() || !!this.nameFilter());
        
        this.filterAndSortDescription = ko.pureComputed(() => {
            const adminPart = this.showAdminCertificates() ? "Admin clearance, " : "";
            const operatorPart = this.showOperatorCertificates() ? "Operator clearance, " : "";
            const userPart = this.showUserCertificates() ? "User clearance, " : "";
            
            if (!adminPart && !operatorPart && !userPart) {
                return `<div class="bg-warning text-warning padding padding-xs">
                            <i class="icon-warning"></i><span>No security clearance is selected in Filter Options</span>
                        </div>`;
            }
           
            const validPart = this.showValidCertificates() ? "Valid, " : "";
            const aboutToExpirePart = this.showAboutToExpireCertificates() ? "About to expire, " : "";
            const expiredPart = this.showExpiredCertificates() ? "Expired, " : "";

            if (!validPart && !aboutToExpirePart && !expiredPart) {
                return `<div class="bg-warning text-warning padding padding-xs">
                            <i class="icon-warning"></i><span>No state is selected in Filter Options</span>
                        </div>`;
            }

            const hasAllClearance = adminPart && operatorPart && userPart;
            const clearancePart = hasAllClearance ? "" : `<strong>Clearance</strong>: ${adminPart}${operatorPart}${userPart}`.slice(0, -2);
            
            const hasAllStates = validPart && aboutToExpirePart && expiredPart;
            const statePart = hasAllStates ? "" : `<strong>State</strong>: ${validPart}${aboutToExpirePart}${expiredPart}`.slice(0, -2);
            
            const databases = this.databasesToShow();
            const databasesNames = databases.map(db => ` ${db}`);
            const databasesPart = databases.length ? `<strong>Databases</strong>: ${databasesNames}` : "";

            const sortPart = this.sortCriteria() ? `<strong>Sorted by</strong>: ${this.sortCriteria()} <br />` : "";
            
            return `${sortPart}${clearancePart}${clearancePart ? "<span class='margin-right-sm'></span>" : ""}${statePart}${clearancePart || statePart ? "<br />" : ""}${databasesPart}`;
        });
        
        this.noCertificateIsVisible = ko.pureComputed(() => {
           const found = this.certificates().find(x => x.Visible());
           return !found; 
        });
    }
    
    private initValidation() {
        this.newPermissionDatabaseName.extend({
            required: true
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

    deleteCertificate(certificate: Raven.Client.ServerWide.Operations.Certificates.CertificateDefinition) {
        this.confirmationMessage("Are you sure?", "Do you want to delete certificate with thumbprint: " + generalUtils.escapeHtml(certificate.Thumbprint) + "", {
            buttons: ["No", "Yes, delete"],
            html: true
        })
            .done(result => {
                if (result.can) {
                    eventsCollector.default.reportEvent("certificates", "delete");
                    new deleteCertificateCommand(certificate.Thumbprint)
                        .execute()
                        .always(() => this.loadCertificates());
                }
            });
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
        this.newPermissionValidationGroup.errors.showAllMessages(false);

        const model = this.model();
        
        if (!this.isValid(model.validationGroup)) {
            return;
        }

        const maybeWarnTask = $.Deferred<void>();
        
        if (this.model().mode() !== "replace" && model.securityClearance() === "ValidUser" && model.permissions().length === 0) {
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
                        this.generateCertPayload(JSON.stringify(model.toGenerateCertificateDto()));

                        new getNextOperationId(null)
                            .execute()
                            .done(operationId => {

                                const targetFrame = $("form#certificate_download_form");
                                targetFrame.attr("action", this.generateCertificateUrl + "?operationId=" + operationId + "&raft-request-id=" + generalUtils.generateUUID());
                                targetFrame.submit();

                                notificationCenter.instance.monitorOperation(null, operationId)
                                    .done(() => {
                                        messagePublisher.reportSuccess("Client certificate was generated and downloaded successfully.");
                                    })
                                    .fail(() => {
                                        notificationCenter.instance.openDetailsForOperationById(null, operationId);
                                    })
                                    .always(() => {
                                        this.spinners.processing(false);
                                        this.onCloseEdit();
                                        this.loadCertificates();
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
                                this.loadCertificates();
                                this.onCloseEdit();
                            });
                        break;

                    case "editExisting":
                        new updateCertificatePermissionsCommand(model)
                            .execute()
                            .always(() => {
                                this.spinners.processing(false);
                                this.loadCertificates();
                                this.onCloseEdit();
                            });
                        break;
                    case "replace":
                        new replaceClusterCertificateCommand(model)
                            .execute()
                            .always(() => {
                                this.spinners.processing(false);
                                this.loadCertificates();
                                this.onCloseEdit();
                            });
                }
            });
    }
    
    private loadCertificates() {
        return new getCertificatesCommand(true)
            .execute()
            .done(certificatesInfo => {
                const mergedCertificates: unifiedCertificateDefinition[] = [];
                
                const secondaryCertificates: Raven.Client.ServerWide.Operations.Certificates.CertificateDefinition[] = [];
                
                certificatesInfo.Certificates.forEach(cert => {
                    if (cert.CollectionPrimaryKey) {
                        secondaryCertificates.push(cert);
                    } else {
                        (cert as unifiedCertificateDefinition).Thumbprints = [cert.Thumbprint];
                        (cert as unifiedCertificateDefinition).Visible = ko.observable<boolean>(true);
                        mergedCertificates.push(cert as unifiedCertificateDefinition);
                    }
                });
                
                this.serverCertificateThumbprint(certificatesInfo.LoadedServerCert);
                this.clientCertificateThumbprint(accessManager.clientCertificateThumbprint());
                
                secondaryCertificates.forEach(cert => {
                    const thumbprint = cert.CollectionPrimaryKey;
                    const primaryCert = mergedCertificates.find(x => x.Thumbprint === thumbprint);
                    primaryCert.Thumbprints.push(cert.Thumbprint);
                });
                
                const orderedCertificates = this.sortByDefaultInternal(mergedCertificates);
                
                this.updateCache(orderedCertificates);
                this.certificates(orderedCertificates);
                
                this.wellKnownAdminCerts(certificatesInfo.WellKnownAdminCerts || []);
                this.filterCertificates();
            });
    }
    
    private updateCache(certificates: Array<unifiedCertificateDefinition>) {
        certificates.forEach((cert: unifiedCertificateDefinitionWithCache) => {
            const expirationDate = moment.utc(cert.NotAfter);
            const expirationDateFormatted = expirationDate.format("YYYY-MM-DD");
            
            const nowPlusMonth = moment.utc().add(1, 'months');
            
            if (expirationDate.isBefore()) {
                cert.expirationText = 'Expired ' + expirationDateFormatted;
                cert.expirationIcon = "icon-danger";
                cert.expirationClass = "text-danger"
            } else if (expirationDate.isAfter(nowPlusMonth)) {
                cert.expirationText = expirationDateFormatted;
                cert.expirationIcon = "icon-clock"; // TODO.. RavenDB-18518
                cert.expirationClass = "";
            } else {
                cert.expirationText = expirationDateFormatted;
                cert.expirationIcon = "icon-warning";
                cert.expirationClass = "text-warning";
            }

            if (cert.NotBefore) {
                const validFromDate = moment.utc(cert.NotBefore);
                cert.validFromText = validFromDate.format("YYYY-MM-DD");
            } else {
                cert.validFromText = "Unavailable" // TODO.. RavenDB-18519
            }
            cert.validFromIcon = "icon-clock"; // TODO.. RavenDB-18518
            cert.validFromClass = "";
        });
    }
    
    onCloseEdit() {
        this.model(null);
    }
    
    deletePermission(permission: certificatePermissionModel) {
        const model = this.model();
        model.permissions.remove(permission);
    }

    useDatabase(databaseName: string) {
        this.newPermissionDatabaseName(databaseName);
        this.addNewPermission();
    }
    
    addNewPermission() {
        if (!this.isValid(this.newPermissionValidationGroup)) {
            return;
        }
        
        const permission = new certificatePermissionModel();
        permission.databaseName(this.newPermissionDatabaseName());
        permission.accessLevel("ReadWrite");
        this.model().permissions.push(permission);
        this.newPermissionDatabaseName("");
        this.newPermissionValidationGroup.errors.showAllMessages(false);
    }

    createDatabaseNameAutocompleter() {
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
    
    copyThumbprint(thumbprint: string) {
        copyToClipboard.copy(thumbprint, "Thumbprint was copied to clipboard.");
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
            if (!this.accessManager.canDeleteClusterAdminCertificate() && securityClearance === "ClusterAdmin") {
                return false;
            }
            
            if (!this.accessManager.canDeleteClusterNodeCertificate() && securityClearance === "ClusterNode") {
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
            if (!this.accessManager.canGenerateClientCertificateForAdmin() && securityClearance === "ClusterAdmin") {
                return false;
            }

            return true;
        });
    }
    
    sortByDefault() {
        this.sortCriteria("");
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
    
    sortByName(mode: string) {
        this.sortCriteria(`Name - ${this.getModeText(mode)}`);
        this.certificates.sort((a, b) =>
            generalUtils.sortAlphaNumeric(a.Name.toLocaleLowerCase(), b.Name.toLocaleLowerCase(), mode as sortMode));
    }

    sortByExpiration(mode: string) {
        this.sortCriteria(`Expiration Date - ${this.getModeText(mode)}`);
        this.certificates.sort((a, b) =>
            generalUtils.sortUTC(a.NotAfter, b.NotAfter, mode as sortMode));
    }

    sortByValidFrom(mode: string) {
        this.sortCriteria(`Valid-From Date - ${this.getModeText(mode)}`);
        this.certificates.sort((a, b) =>
            generalUtils.sortUTC(a.NotBefore, b.NotBefore, mode as sortMode));
    }
    
    getModeText(mode: string) {
        return mode === "asc" ? "Ascending" : "Descending";
    }

    createDatabaseNameAutoCompleterForFilter() {
        return ko.pureComputed(() => {
            const key = this.inputDatabase();

            const options = this.databases().map(x => x.name);

            const usedOptions = this.databasesToShow().filter(k => k !== key);

            const filteredOptions = _.difference(options, usedOptions);

            if (key) {
                return filteredOptions.filter(x => x.toLowerCase().includes(key.toLowerCase()));
            } else {
                return filteredOptions;
            }
        });
    }

    addDatabase() {
        this.addWithBlink(this.inputDatabase());
    }

    removeDatabase(database: string) {
        this.databasesToShow.remove(database);
    }

    addWithBlink(databaseName: string) {
        this.databasesToShow.unshift(databaseName);
        
        this.inputDatabase("");
        
        $(".collection-list li").first().addClass("blink-style");
    }
}

export = certificates;
