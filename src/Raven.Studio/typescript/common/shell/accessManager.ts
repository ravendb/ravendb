/// <reference path="../../../typings/tsd.d.ts"/>
import activeDatabaseTracker = require("common/shell/activeDatabaseTracker");

class accessManager {

    static default = new accessManager();
    
    static clientCertificateThumbprint = ko.observable<string>();
    
    static databasesAccess: dictionary<Raven.Client.ServerWide.Operations.Certificates.DatabaseAccess> = {};
    
    securityClearance = ko.observable<Raven.Client.ServerWide.Operations.Certificates.SecurityClearance>();

    private allLevels = ko.pureComputed(() => true);
    
    // cluster node has the same privileges as cluster admin
    isClusterAdminOrClusterNode = ko.pureComputed(() => {
        const clearance = this.securityClearance();
        return clearance === "ClusterAdmin" || clearance === "ClusterNode";
    });
    
    isOperatorOrAbove = ko.pureComputed(() => {
         const clearance = this.securityClearance();
         return clearance === "ClusterAdmin" || clearance === "ClusterNode" || clearance === "Operator";
    });

    isValidUser = ko.pureComputed<boolean>(() => this.securityClearance() === "ValidUser");


    isAdminByDbName(dbName: string) {
        return this.getDatabaseAccessLevelTextByDbName(dbName) === "Admin";
    }
    
    getEffectiveDatabaseAccessLevel(dbName: string) {
        if (this.isOperatorOrAbove()) {
            return "Admin";
        }
        
        return accessManager.databasesAccess[dbName];
    }

    getDatabaseAccessLevelTextByDbName(dbName: string) {
        const accessLevel = this.getEffectiveDatabaseAccessLevel(dbName);
        return accessLevel ? this.getAccessLevelText(accessLevel) : null;
    }
    
    getAccessLevelText(accessLevel: Raven.Client.ServerWide.Operations.Certificates.DatabaseAccess) {
        switch (accessLevel) {
            case "Admin":
                return "Admin";
            case "ReadWrite":
                return "Read/Write";
            case "Read":
                return "Read Only";
        }
    }

    getAccessColorByDbName(dbName: string) {
        const accessLevel = this.getEffectiveDatabaseAccessLevel(dbName);
        return this.getAccessColor(accessLevel);
    }
    
    getAccessColor(accessLevel: Raven.Client.ServerWide.Operations.Certificates.DatabaseAccess) {
        switch (accessLevel) {
            case "Admin":
                return "text-success";
            case "ReadWrite":
                return "text-warning";
            case "Read":
                return "text-danger";
        }
    }

    getAccessIconByDbName(dbName: string) {
        const accessLevel = this.getEffectiveDatabaseAccessLevel(dbName);
        return this.getAccessIcon(accessLevel);
    }
    
    getAccessIcon(accessLevel: Raven.Client.ServerWide.Operations.Certificates.DatabaseAccess) {
        switch (accessLevel) {
            case "Admin":
                return "icon-access-admin";
            case "ReadWrite":
                return "icon-access-read-write";
            case "Read":
                return "icon-access-read";
        }
    }
    
    activeDatabaseAccessLevel = ko.pureComputed<Raven.Client.ServerWide.Operations.Certificates.DatabaseAccess>(() => {
        const activeDatabase = activeDatabaseTracker.default.database();
        if (activeDatabase) {
            return this.getEffectiveDatabaseAccessLevel(activeDatabase.name);
        } 
        
        return null;
    });
    
    isReadOnlyAccess = ko.pureComputed(() => this.activeDatabaseAccessLevel() === 'Read');
    
    isReadWriteAccessOrAbove = ko.pureComputed(() => {
        const accessLevel = this.activeDatabaseAccessLevel();
        return accessLevel === 'ReadWrite' || accessLevel === 'Admin';
    });
    
    isAdminAccessOrAbove = ko.pureComputed(() => {
        const accessLevel = this.activeDatabaseAccessLevel();
        return accessLevel === 'Admin';
    });
    
    canHandleOperation(requiredAccess: Raven.Client.ServerWide.Operations.Certificates.DatabaseAccess) {
        return ko.pureComputed(() => {
            const activeDatabaseAccess = this.activeDatabaseAccessLevel();
            
            if (!activeDatabaseAccess) {
                return false;
            }
            
            if (activeDatabaseAccess === 'Read' &&
                (requiredAccess === 'ReadWrite' || requiredAccess === 'Admin')) {
                return false;
            }

            if (activeDatabaseAccess === 'ReadWrite' && requiredAccess === 'Admin') {
                return false;
            }

            return true;
        })
    }
    
    private createSecurityRule(enabledPredicate: KnockoutObservable<boolean>, requiredRoles: string) {
        return ko.pureComputed(() => {
            if (enabledPredicate()) {
                return undefined;
            }
            return this.getRuleHtml("Insufficient security clearance", requiredRoles, this.securityClearance());
        });
    }

    private createDatabaseSecurityRule(enabledPredicate: KnockoutObservable<boolean>, 
                                       requiredDatabaseAccess: string) {
        return ko.pureComputed(() => {
            if  (enabledPredicate()) {
                return undefined;
            }

            if (!activeDatabaseTracker.default.database()) {
                return undefined;
            }
            
            return this.getRuleHtml("Insufficient database access", requiredDatabaseAccess,
                                    this.getDatabaseAccessLevelTextByDbName(activeDatabaseTracker.default.database().name));
        });
    }
    
    
    private getRuleHtml(title: string, required: string, actual: string) {
        return `<div class="text-left">
                            <h4>${title}</h4>
                            <ul>
                                <li>Required: <strong>${required}</strong></li>
                                <li>Actual: <strong>${actual}</strong></li>
                            </ul>
                        </div>`;
    }

    static activeDatabaseTracker = activeDatabaseTracker.default;

    disableIfNotClusterAdminOrClusterNode = this.createSecurityRule(this.isClusterAdminOrClusterNode, "Cluster Admin");
    disableIfNotOperatorOrAbove = this.createSecurityRule(this.isOperatorOrAbove, "Cluster Admin or Operator");
    
    disableIfNotAdminAccessPerDatabaseOrAbove = this.createDatabaseSecurityRule(this.isAdminAccessOrAbove, 'Admin');
    disableIfNotReadWriteAccessPerDatabaseOrAbove = this.createDatabaseSecurityRule(this.isReadWriteAccessOrAbove, 'Read/Write');
    
    dashboardView = {
        showCertificatesLink: this.isOperatorOrAbove
    };
    
    clusterView = {
        canAddNode: this.isClusterAdminOrClusterNode,
        canDeleteNode: this.isClusterAdminOrClusterNode,
        showCoresInfo: this.isClusterAdminOrClusterNode,
        canDemotePromoteNode: this.isClusterAdminOrClusterNode
    };
    
    aboutView = {
        canReplaceLicense: this.isClusterAdminOrClusterNode,
        canForceUpdate: this.isClusterAdminOrClusterNode,
        canRenewLicense: this.isClusterAdminOrClusterNode,
        canRegisterLicense: this.isClusterAdminOrClusterNode
    };
    
    databasesView = {
        canCreateNewDatabase: this.isOperatorOrAbove,
        canSetState: this.isOperatorOrAbove,
        canDelete: this.isOperatorOrAbove,
        canDisableEnableDatabase: this.isOperatorOrAbove,
        canDisableIndexing: this.isOperatorOrAbove,
        canCompactDatabase: this.isOperatorOrAbove
    };
    
    certificatesView = {
        canRenewLetsEncryptCertificate: this.isClusterAdminOrClusterNode,
        canDeleteClusterNodeCertificate: this.isClusterAdminOrClusterNode,
        canDeleteClusterAdminCertificate: this.isClusterAdminOrClusterNode,
        canGenerateClientCertificateForAdmin: this.isClusterAdminOrClusterNode
    };

    mainMenu = {
        showManageServerMenuItem: this.allLevels
    };
    
    manageServerMenu = {
        disableClusterMenuItem: undefined as KnockoutComputed<string>,
        disableClientConfigurationMenuItem: this.disableIfNotOperatorOrAbove,
        disableStudioConfigurationMenuItem: this.disableIfNotOperatorOrAbove,
        disableAdminJSConsoleMenuItem: this.disableIfNotClusterAdminOrClusterNode,
        disableCertificatesMenuItem: this.disableIfNotOperatorOrAbove,
        disableServerWideTasksMenuItem: this.disableIfNotClusterAdminOrClusterNode,
        disableServerWideCustomAnalyzersMenuItem: this.disableIfNotClusterAdminOrClusterNode,
        disableServerWideCustomSortersMenuItem: this.disableIfNotClusterAdminOrClusterNode,
        disableAdminLogsMenuItem: this.disableIfNotOperatorOrAbove,
        disableTrafficWatchMenuItem: this.disableIfNotOperatorOrAbove,
        disableGatherDebugInfoMenuItem: this.disableIfNotOperatorOrAbove,
        disableSystemStorageReport: this.disableIfNotOperatorOrAbove,
        disableSystemIoStats: this.disableIfNotOperatorOrAbove,
        disableAdvancedMenuItem: this.disableIfNotOperatorOrAbove,
        disableCaptureStackTraces: this.disableIfNotOperatorOrAbove,
        enableRecordTransactionCommands: this.isOperatorOrAbove
    };
    
    databaseSettingsMenu = {
        showDatabaseSettingsMenuItem: this.isOperatorOrAbove,
        showDatabaseRecordMenuItem: this.isOperatorOrAbove,
        showDatabaseIDsMenuItem: this.isOperatorOrAbove,
        disableConnectionStringsMenuItem: this.disableIfNotAdminAccessPerDatabaseOrAbove,
        disableConflictResolutionMenuItem: this.disableIfNotAdminAccessPerDatabaseOrAbove
    };
    
    databaseDocumentsMenu = {
        disablePatchMenuItem: this.disableIfNotReadWriteAccessPerDatabaseOrAbove
    }

    databaseTasksMenu = {
        disableImportDataItem: this.disableIfNotReadWriteAccessPerDatabaseOrAbove,
        disableCreateSampleDataItem: this.disableIfNotReadWriteAccessPerDatabaseOrAbove,
        disableExportDatabaseItem: this.disableIfNotReadWriteAccessPerDatabaseOrAbove
    }
}

export = accessManager;
