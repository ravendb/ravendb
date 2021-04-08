/// <reference path="../../../typings/tsd.d.ts"/>
import activeDatabaseTracker = require("common/shell/activeDatabaseTracker");

class accessManager {

    static default = new accessManager();
    
    static clientCertificateThumbprint = ko.observable<string>();
    
    static databasesAccess: dictionary<Raven.Client.ServerWide.Operations.Certificates.DatabaseAccess> = {};
    
    securityClearance = ko.observable<Raven.Client.ServerWide.Operations.Certificates.SecurityClearance>();

    private allLevels = ko.pureComputed(() => true);
    
    // cluster node has the same privileges as cluster admin
    clusterAdminOrClusterNode = ko.pureComputed(() => this.securityClearance() === "ClusterAdmin" || this.securityClearance() === "ClusterNode");
        
    isUserClearance = ko.pureComputed<boolean>(() => this.securityClearance() === "ValidUser");
    
    operatorAndAbove = ko.pureComputed(() => { 
         const clearance = this.securityClearance();
         return clearance === "ClusterAdmin" || clearance === "ClusterNode" || clearance === "Operator";
    });    
    
    getDatabaseAccessLevel(dbName: string) {
        if (this.operatorAndAbove()) {
            return "Admin";
        }
        
        return accessManager.databasesAccess[dbName];
    }

    getDatabaseAccessLevelText(dbName: string) {
        const accessLevel = this.getDatabaseAccessLevel(dbName);
        if (accessLevel) {
            switch (accessLevel) {
                case "Admin":
                    return "Admin access";
                case "ReadWrite":
                    return "Read/Write access";
                case "Read":
                    return "Read Only access";
            }
        }
        
        return null;
    }
    
    activeDatabaseAccessLevel = ko.pureComputed<Raven.Client.ServerWide.Operations.Certificates.DatabaseAccess>(() => {
        const activeDatabase = activeDatabaseTracker.default.database();
        if (activeDatabase) {
            return this.getDatabaseAccessLevel(activeDatabase.name);
        } 
        
        return null;
    });
    
    isReadOnlyAccess = ko.pureComputed(() => this.activeDatabaseAccessLevel() === 'Read');
    
    canHandleOperation(requiredAccess: Raven.Client.ServerWide.Operations.Certificates.DatabaseAccess)  {
        return ko.pureComputed(() => {
            if (this.activeDatabaseAccessLevel() === 'Read') {
                return false;
            }

            if (this.activeDatabaseAccessLevel() === 'ReadWrite' && requiredAccess === 'Admin') {
                return false;
            }

            return true;
        })
    }
    
    private createSecurityRule(enabledPredicate: KnockoutObservable<boolean>, requiredRoles: string) {
        return ko.pureComputed(() => {
            const enabled = enabledPredicate();
            const clearance = this.securityClearance();
            
            if (enabled) {
                return undefined;
            }
            return this.getRuleHtml("Insufficient security clearance", requiredRoles, clearance);
        });
    }

    private createDatabaseSecurityRule(requiredDatabaseAccess: Raven.Client.ServerWide.Operations.Certificates.DatabaseAccess) {
        return ko.pureComputed(() => {
            if (requiredDatabaseAccess === 'Admin' && this.operatorAndAbove()) {
                return undefined;
            }
            
            if (this.canHandleOperation(requiredDatabaseAccess)) {
                return this.getRuleHtml("Insufficient database access", requiredDatabaseAccess, this.activeDatabaseAccessLevel());
            }
            
            return undefined;
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

    disableIfNotClusterAdminOrClusterNode = this.createSecurityRule(this.clusterAdminOrClusterNode, "Cluster Admin");
    disableIfNotOperatorOrAbove = this.createSecurityRule(this.operatorAndAbove, "Cluster Admin or Operator");
    
    disableIfNotAdminAccessPerDatabase = this.createDatabaseSecurityRule('Admin');
    disableIfNotReadWriteAccessPerDatabase = this.createDatabaseSecurityRule('ReadWrite');
    
    dashboardView = {
        showCertificatesLink: this.operatorAndAbove
    };
    
    clusterView = {
        canAddNode: this.clusterAdminOrClusterNode,
        canDeleteNode: this.clusterAdminOrClusterNode,
        showCoresInfo: this.clusterAdminOrClusterNode,
        canDemotePromoteNode: this.clusterAdminOrClusterNode
    };
    
    aboutView = {
        canReplaceLicense: this.clusterAdminOrClusterNode,
        canForceUpdate: this.clusterAdminOrClusterNode,
        canRenewLicense: this.clusterAdminOrClusterNode,
        canRegisterLicense: this.clusterAdminOrClusterNode
    };
    
    databasesView = {
        canCreateNewDatabase: this.operatorAndAbove,
        canSetState: this.operatorAndAbove,
        canDelete: this.operatorAndAbove,
        canDisableEnableDatabase: this.operatorAndAbove,
        canDisableIndexing: this.operatorAndAbove,
        canCompactDatabase: this.operatorAndAbove
    };
    
    certificatesView = {
        canRenewLetsEncryptCertificate: this.clusterAdminOrClusterNode,
        canDeleteClusterNodeCertificate: this.clusterAdminOrClusterNode,
        canDeleteClusterAdminCertificate: this.clusterAdminOrClusterNode,
        canGenerateClientCertificateForAdmin: this.clusterAdminOrClusterNode
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
        enableRecordTransactionCommands: this.operatorAndAbove
    };
    
    databaseSettingsMenu = {
        showDatabaseSettingsMenuItem: this.operatorAndAbove,
        showDatabaseRecordMenuItem: this.operatorAndAbove,
        showDatabaseIDsMenuItem: this.operatorAndAbove,
        showConnectionStringsMenuItem: this.operatorAndAbove,
        disableConnectionStringsMenuItem: this.disableIfNotAdminAccessPerDatabase,
        disableConflictResolutionMenuItem: this.disableIfNotClusterAdminOrClusterNode
    };
    
    databaseDocumentsMenu = {
        disablePatchMenuItem: this.disableIfNotReadWriteAccessPerDatabase
    }

    databaseTasksMenu = {
        disableImportDataItem: this.disableIfNotReadWriteAccessPerDatabase,
        disableCreateSampleDataItem: this.disableIfNotReadWriteAccessPerDatabase,
        disableExportDatabaseItem: this.disableIfNotReadWriteAccessPerDatabase
    }
}

export = accessManager;
