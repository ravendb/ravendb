/// <reference path="../../../typings/tsd.d.ts"/>

class accessManager {

    static default = new accessManager();
    
    static clientCertificateThumbprint = ko.observable<string>();
    
    securityClearance = ko.observable<Raven.Client.ServerWide.Operations.Certificates.SecurityClearance>();

    private allLevels = ko.pureComputed(() => true);
    
    // cluster node has the same privileges as cluster admin
    clusterAdminOrClusterNode = ko.pureComputed(() => this.securityClearance() === "ClusterAdmin" || this.securityClearance() === "ClusterNode");
        
    operatorAndAbove = ko.pureComputed(() => { 
         const clearance = this.securityClearance();
         return clearance === "ClusterAdmin" || clearance === "ClusterNode" || clearance === "Operator";
    });
    
    private createSecurityRule(enabledPredicate: KnockoutObservable<boolean>, requiredRoles: string) {
        return ko.pureComputed(() => {
            const enabled = enabledPredicate();
            const clearance = this.securityClearance();
            if (enabled) {
                return undefined;
            } else {
                return "Insufficient security clearance. <br /> Required: " + requiredRoles + "<br />Current: " + clearance;
            }
        });
    }

    disableIfNotClusterAdminOrClusterNode = this.createSecurityRule(this.clusterAdminOrClusterNode, "Cluster Admin");

    disableIfNotOperatorOrAbove = this.createSecurityRule(this.operatorAndAbove, "Cluster Admin or Operator");
    
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
        canUpgrade: this.clusterAdminOrClusterNode
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
        showConnectionStringsMenuItem: this.operatorAndAbove,
        enableConnectionStringsMenuItem: this.clusterAdminOrClusterNode,
        enableConflictResolutionMenuItem: this.clusterAdminOrClusterNode
    };
}

export = accessManager;
