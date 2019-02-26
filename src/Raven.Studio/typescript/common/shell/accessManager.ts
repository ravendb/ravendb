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
    
    // *** Views Access *** //
    
    dashboardView = {
        showCPUAndMemGraph: this.operatorAndAbove
    };
    
    clusterView = {
        canAddNode: this.clusterAdminOrClusterNode,
        canDeleteNode: this.clusterAdminOrClusterNode,
        showCoresInfo: this.clusterAdminOrClusterNode
    };
    
    aboutView = {
        canReplaceLicense: this.clusterAdminOrClusterNode, 
        canForceUpdate: this.clusterAdminOrClusterNode,
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

    // *** Menus Access *** //
    
    mainMenu = {
        showManageServerMenuItem: this.operatorAndAbove
    };
    
    manageServerMenu = {
        showAdminJSConsoleMenuItem: this.clusterAdminOrClusterNode,
        enableClusterMenuItem: this.allLevels,
        enableClientConfigurationMenuItem: this.clusterAdminOrClusterNode,
        enableStudioConfigurationMenuItem: this.clusterAdminOrClusterNode,
        enableAdminJSConsoleMenuItem: this.clusterAdminOrClusterNode,
        enableCertificatesMenuItem: this.clusterAdminOrClusterNode,
        enableAdminLogsMenuItem: this.clusterAdminOrClusterNode,
        enableTrafficWatchMenuItem: this.clusterAdminOrClusterNode,
        enableGatherDebugInfoMenuItem: this.clusterAdminOrClusterNode,
        enableCaptureStackTraces: this.clusterAdminOrClusterNode,
        enableAdvancedMenuItem: this.clusterAdminOrClusterNode
    };
    
    databaseSettingsMenu = {
        showDatabaseRecordMenuItem: this.operatorAndAbove,
        showConnectionStringsMenuItem: this.operatorAndAbove,
        enableConnectionStringsMenuItem: this.clusterAdminOrClusterNode, 
        enableConflictResolutionMenuItem: this.clusterAdminOrClusterNode
    };
}

export = accessManager;
