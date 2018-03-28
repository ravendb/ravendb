/// <reference path="../../../typings/tsd.d.ts"/>

class accessManager {

    static default = new accessManager();
    
    securityClearance = ko.observable<Raven.Client.ServerWide.Operations.Certificates.SecurityClearance>();

    clusterAdmin = ko.pureComputed(() => this.securityClearance() === "ClusterAdmin");    
    private clusterNode = ko.pureComputed(() => this.securityClearance() === "ClusterNode");
    private operator = ko.pureComputed(() => this.securityClearance() === "Operator");
    validUser = ko.pureComputed(() => this.securityClearance() === "ValidUser");
    private unauthenticatedClient = ko.pureComputed(() => this.securityClearance() === "UnauthenticatedClients");

    private validUserAndBelow = ko.pureComputed(()=> this.validUser() || this.unauthenticatedClient());
    private OperatorAndBelow = ko.pureComputed(()=> this.operator() || this.validUser() || this.unauthenticatedClient());   
    private clusterNodeAndBelow = ko.pureComputed(()=> this.clusterNode() || this.operator() || this.validUser() || this.unauthenticatedClient()); 
    private clusterAdminAndBelow = ko.pureComputed(()=> this.clusterAdmin() || this.clusterNode() || this.operator() || this.validUser() || this.unauthenticatedClient());
    
    private operatorAndAbove = ko.pureComputed(() => this.clusterAdmin() || this.clusterNode() || this.operator());
    private clusterNodeAndAbove = ko.pureComputed(() => this.clusterAdmin() || this.clusterNode());
         
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
        enableClusterMenuItem: this.clusterAdminAndBelow,
        enableClientConfigurationMenuItem: this.clusterAdmin,
        enableAdminJSConsoleMenuItem: this.clusterAdmin,
        enableCertificatesMenuItem: this.clusterAdmin,
        enableAdminLogsMenuItem: this.clusterAdmin,
        enableTrafficWatchMenuItem: this.clusterAdmin,
        enableGatherDebugInfoMenuItem: this.clusterAdmin,
        enableAdvancedMenuItem: this.clusterAdmin
    }
    
    databaseSettingsMenu = {
        showDatabaseRecordMenuItem: this.operatorAndAbove,
        enableConnectionStringsMenuItem: this.clusterAdmin,
        enableConflictResolutionMenuItem: this.clusterAdmin
    }
}

export = accessManager;
