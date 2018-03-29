/// <reference path="../../../typings/tsd.d.ts"/>

class accessManager {

    static default = new accessManager();
    
    securityClearance = ko.observable<Raven.Client.ServerWide.Operations.Certificates.SecurityClearance>();

    private allLevels = ko.pureComputed(() =>  true);
    clusterAdmin = ko.pureComputed(() => this.securityClearance() === "ClusterAdmin");
        
    operatorAndAbove = ko.pureComputed(() => { 
         const clearance = this.securityClearance();
         return clearance === "ClusterAdmin" || clearance === "ClusterNode" || clearance === "Operator";
    });
    
    // *** Views Access *** //
    
    dashboardView = {
        showCPUAndMemGraph: this.operatorAndAbove
    };
    
    clusterView = {
        canAddNode: this.clusterAdmin,
        canDeleteNode: this.clusterAdmin,
        showCoresInfo: this.clusterAdmin
    };
    
    aboutView = {
        canReplaceLicense: this.clusterAdmin, 
        canForceUpdate: this.clusterAdmin,
        canUpgrade: this.clusterAdmin
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
        canDeleteClusterNodeCertificate: this.clusterAdmin,
        canDeleteClusterAdminCertificate: this.clusterAdmin,
        canGenerateClientCertificateForAdmin: this.clusterAdmin
    };

    // *** Menus Access *** //
    
    mainMenu = {
        showManageServerMenuItem: this.operatorAndAbove
    };
    
    manageServerMenu = {
        showAdminJSConsoleMenuItem: this.clusterAdmin,
        enableClusterMenuItem: this.allLevels,
        enableClientConfigurationMenuItem: this.clusterAdmin,
        enableAdminJSConsoleMenuItem: this.clusterAdmin,
        enableCertificatesMenuItem: this.clusterAdmin,
        enableAdminLogsMenuItem: this.clusterAdmin,
        enableTrafficWatchMenuItem: this.clusterAdmin,
        enableGatherDebugInfoMenuItem: this.clusterAdmin,
        enableAdvancedMenuItem: this.clusterAdmin
    };
    
    databaseSettingsMenu = {
        showDatabaseRecordMenuItem: this.operatorAndAbove,
        showConnectionStringsMenuItem: this.operatorAndAbove,
        enableConnectionStringsMenuItem: this.clusterAdmin, 
        enableConflictResolutionMenuItem: this.clusterAdmin
    };
}

export = accessManager;
