/// <reference path="../../../typings/tsd.d.ts"/>
import activeDatabaseTracker = require("common/shell/activeDatabaseTracker");

class accessManager {

    static default = new accessManager();
    
    static clientCertificateThumbprint = ko.observable<string>();
   
    static databasesAccess: dictionary<databaseAccessLevel> = {};
    
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

    isAdminByDbName(dbName: string): boolean {
        return this.getEffectiveDatabaseAccessLevel(dbName) === "DatabaseAdmin";
    }
    
    getEffectiveDatabaseAccessLevel(dbName: string): databaseAccessLevel {
        if (this.isOperatorOrAbove()) {
            return "DatabaseAdmin";
        }
        
        return accessManager.databasesAccess[dbName];
    }

    getDatabaseAccessLevelTextByDbName(dbName: string): string {
        const accessLevel = this.getEffectiveDatabaseAccessLevel(dbName);
        return accessLevel ? accessManager.getAccessLevelText(accessLevel) : null;
    }
    
    static getAccessLevelText(accessLevel: accessLevel): string {
        switch (accessLevel) {
            case "ClusterNode":
            case "ClusterAdmin":
                return "Cluster Admin/Node";
            case "Operator":
                return "Operator";
            case "DatabaseAdmin":
                return "Admin";
            case "DatabaseReadWrite":
                return "Read/Write";
            case "DatabaseRead":
                return "Read Only";
        }
    }

    getAccessColorByDbName(dbName: string): string {
        const accessLevel = this.getEffectiveDatabaseAccessLevel(dbName);
        return this.getAccessColor(accessLevel);
    }
    
    getAccessColor(accessLevel: databaseAccessLevel): string {
        switch (accessLevel) {
            case "DatabaseAdmin":
                return "text-success";
            case "DatabaseReadWrite":
                return "text-warning";
            case "DatabaseRead":
                return "text-danger";
        }
    }

    getAccessIconByDbName(dbName: string): string {
        const accessLevel = this.getEffectiveDatabaseAccessLevel(dbName);
        return this.getAccessIcon(accessLevel);
    }
    
    getAccessIcon(accessLevel: databaseAccessLevel): string {
        switch (accessLevel) {
            case "DatabaseAdmin":
                return "icon-access-admin";
            case "DatabaseReadWrite":
                return "icon-access-read-write";
            case "DatabaseRead":
                return "icon-access-read";
        }
    }
    
    activeDatabaseEffectiveAccessLevel = ko.pureComputed<databaseAccessLevel>(() => {
        const activeDatabase = activeDatabaseTracker.default.database();
        if (activeDatabase) {
            return this.getEffectiveDatabaseAccessLevel(activeDatabase.name);
        }
        return null;
    });
    
    isReadOnlyAccess = ko.pureComputed(() => this.activeDatabaseEffectiveAccessLevel() === "DatabaseRead");
    
    isReadWriteAccessOrAbove = ko.pureComputed(() => {
        const accessLevel = this.activeDatabaseEffectiveAccessLevel();
        return accessLevel === "DatabaseReadWrite" || accessLevel === "DatabaseAdmin";
    });
    
    isAdminAccessOrAbove = ko.pureComputed(() => {
        const accessLevel = this.activeDatabaseEffectiveAccessLevel();
        return accessLevel === "DatabaseAdmin";
    });
    
    static isSecurityClearanceLevel(access: accessLevel): access is securityClearance {
        return access === "ClusterAdmin" || access === "ClusterNode" || access === "Operator" || access === "ValidUser";
    }
    
    static canHandleOperation(requiredAccess: accessLevel, dbName: string = null): boolean {
        const actualAccessLevel = accessManager.default.isOperatorOrAbove()
            ? accessManager.default.securityClearance()
            : accessManager.databasesAccess[dbName];
        
        if (!actualAccessLevel) {
            return false;
        }
        
        const clusterAdminOrNode = actualAccessLevel === "ClusterAdmin" || actualAccessLevel === "ClusterNode";
        const operator = actualAccessLevel === "Operator";
        const dbAdmin = actualAccessLevel === "DatabaseAdmin";
        const dbReadWrite = actualAccessLevel === "DatabaseReadWrite";
        const dbRead = actualAccessLevel === "DatabaseRead";
        
        switch (requiredAccess) {
            case "ClusterAdmin":
            case "ClusterNode":
                return clusterAdminOrNode;
            case "Operator":
                return clusterAdminOrNode || operator;
            case "DatabaseAdmin":
                return clusterAdminOrNode || operator || dbAdmin;
            case "DatabaseReadWrite":
                return clusterAdminOrNode || operator || dbAdmin || dbReadWrite;
            case "DatabaseRead":
                return clusterAdminOrNode || operator || dbAdmin || dbReadWrite || dbRead;
            default: 
                return false;
        }
    }
    
    static getDisableReasonHtml(requiredAccess: accessLevel) {
        const securityClearance = accessManager.isSecurityClearanceLevel(requiredAccess);
        const title = securityClearance ? "Insufficient security clearance" : "Insufficient database access";

        const requiredText = accessManager.getAccessLevelText(requiredAccess);

        return `<div class="text-left">
                    <h4>${title}</h4>
                    <ul>
                        <li>Required: <strong>${requiredText}</strong></li>
                    </ul>
                </div>`;
    }

    static activeDatabaseTracker = activeDatabaseTracker.default;
    
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
}

export = accessManager;
