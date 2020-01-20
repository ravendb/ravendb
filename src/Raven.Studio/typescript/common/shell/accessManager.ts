/// <reference path="../../../typings/tsd.d.ts"/>

class accessManager {

    static default = new accessManager();
    
    securityClearance = ko.observable<Raven.Client.ServerWide.Operations.Certificates.SecurityClearance>();

    private allLevels = ko.pureComputed(() =>  true);
    
    // cluster node has the same privileges as cluster admin
    clusterAdminOrClusterNode = ko.pureComputed(() => this.securityClearance() === "ClusterAdmin" || this.securityClearance() === "ClusterNode");
        
    operatorAndAbove = ko.pureComputed(() => { 
         const clearance = this.securityClearance();
         return clearance === "ClusterAdmin" || clearance === "ClusterNode" || clearance === "Operator";
    });
    
    disableIfNotClusterAdminOrClusterNode = ko.pureComputed<string>(() => {
        const enabled = this.clusterAdminOrClusterNode();
        const clearance = this.securityClearance();
        if (enabled) {
            return undefined;
        } else {
            return "Insufficient security clearance. <br /> Required: Cluster Admin<br />Current: " + clearance;
        }
    });
    
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
        showManageServerMenuItem: this.operatorAndAbove
    };
    
    manageServerMenu = {
        showAdminJSConsoleMenuItem: this.clusterAdminOrClusterNode,
        disableClusterMenuItem: undefined as KnockoutComputed<string>,
        disableClientConfigurationMenuItem: this.disableIfNotClusterAdminOrClusterNode,
        disableStudioConfigurationMenuItem: this.disableIfNotClusterAdminOrClusterNode,
        disableAdminJSConsoleMenuItem: this.disableIfNotClusterAdminOrClusterNode,
        disableCertificatesMenuItem: this.disableIfNotClusterAdminOrClusterNode,
        disableServerWideBackupMenuItem: this.disableIfNotClusterAdminOrClusterNode,
        disableAdminLogsMenuItem: this.disableIfNotClusterAdminOrClusterNode,
        disableTrafficWatchMenuItem: this.disableIfNotClusterAdminOrClusterNode,
        disableGatherDebugInfoMenuItem: this.disableIfNotClusterAdminOrClusterNode,
        disableSystemStorageReport: this.disableIfNotClusterAdminOrClusterNode,
        disableSystemIoStats: this.disableIfNotClusterAdminOrClusterNode,
        disableAdvancedMenuItem: this.disableIfNotClusterAdminOrClusterNode,
        disableCaptureStackTraces: this.disableIfNotClusterAdminOrClusterNode,
        enableRecordTransactionCommands: this.clusterAdminOrClusterNode
    };
    
    databaseSettingsMenu = {
        showDatabaseRecordMenuItem: this.operatorAndAbove,
        showConnectionStringsMenuItem: this.operatorAndAbove,
        enableConnectionStringsMenuItem: this.clusterAdminOrClusterNode, 
        enableConflictResolutionMenuItem: this.clusterAdminOrClusterNode
    };
}

export = accessManager;
