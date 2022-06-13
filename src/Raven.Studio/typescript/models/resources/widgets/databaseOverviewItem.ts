
import moment = require("moment");

class databaseOverviewItem implements databaseAndNodeAwareStats {
    database: string;
    nodeTag: string;
    relevant: boolean;
    disabled: boolean;
    online: boolean;
    documents: number;
    alerts: number;
    performanceHints: number;
    indexes: number;
    erroredIndexes: number;
    indexingErrors: number;
    ongoingTasks: number;
    backupInfo: Raven.Client.ServerWide.Operations.BackupInfo;
    
    noData: boolean;
    
    hideDatabaseName: boolean;
    even: boolean = false;
    
    constructor(nodeTag: string, data: Raven.Server.Dashboard.DatabaseInfoItem) {
        this.nodeTag = nodeTag;
        this.hideDatabaseName = false;
        
        if (data) {
            this.noData = false;
            this.database = data.Database;
            this.relevant = !data.Irrelevant;
            this.disabled = data.Disabled;
            this.online = data.Online
            this.documents = data.DocumentsCount;
            this.alerts = data.AlertsCount;
            this.performanceHints = data.PerformanceHintsCount;
            this.indexes = data.IndexesCount;
            this.erroredIndexes = data.ErroredIndexesCount;
            this.indexingErrors = data.IndexingErrorsCount;
            this.ongoingTasks = data.OngoingTasksCount;
            this.backupInfo = data.BackupInfo;
        } else {
            this.noData = true;
        }
    }

    static noData(nodeTag: string, database: string): databaseOverviewItem {
        const item = new databaseOverviewItem(nodeTag, null);
        item.database = database;
        return item;
    }

    static commonData(item: databaseOverviewItem) {
        const commonItem = new databaseOverviewItem(null, null);
        commonItem.relevant = true; 
        
        commonItem.database = item.database;
        commonItem.documents = item.documents;
        commonItem.indexes = item.indexes;
        commonItem.ongoingTasks = item.ongoingTasks;
        commonItem.backupInfo = item.backupInfo;
        commonItem.disabled = item.disabled;
        
        return commonItem;
    }

    erroredIndexesDataForHtml(): iconPlusText[] {
        if (!this.erroredIndexes) {
            return [];
        }

        const textValue = this.erroredIndexes.toLocaleString();

        return [{
            title: `${textValue} errored ${this.erroredIndexes > 1 ? "indexes" : "index"}`,
            text: textValue,
            iconClass: "icon-danger",
            textClass: "text-danger"
        }];
    }
    
    indexingErrorsDataForHtml(): iconPlusText[] {
        if (!this.indexingErrors) {
            return [];
        }
        
        const textValue = this.indexingErrors.toLocaleString();
        
        return [{
            title: `${textValue} indexing ${this.indexingErrors > 1 ? "errors" : "error"}`,
            text: textValue,
            iconClass: "icon-danger",
            textClass: "text-danger"
        }];
    }

    alertsDataForHtml(): iconPlusText[] {
        if (!this.alerts && !this.performanceHints) {
            return [];
        }
        
        const alertsData: iconPlusText[] = [];
        
        if (this.alerts) {
            const textValue = this.alerts.toLocaleString();
            alertsData.push({
                title: `${textValue} ${this.alerts > 1 ? "alerts" : "alert"}`,
                text: textValue,
                iconClass: "icon-warning",
                textClass: "text-warning"
            });
        }
       
        if (this.performanceHints) {
            const textValue = this.performanceHints.toLocaleString();
            alertsData.push({
                title: `${textValue} performance ${this.performanceHints > 1 ? "hints" : "hint" }`,
                text: textValue,
                iconClass: "icon-info",
                textClass: "text-info"
            });
        }
        
        return alertsData;
    }
    
    backupDataForHtml(): iconPlusText[] {
        if (!this.backupInfo) {
            return [{
               title: "No backup task defined",
               text: "",
               iconClass: "icon-cancel",
               textClass: "text-danger" 
            }];
        } 
        
        if (!this.backupInfo.LastBackup) {
            return [{
                title: "Never backed up",
                text: "",
                iconClass: "icon-warning",
                textClass: "text-warning"
            }];
        }
        
        return [{
            title: "Backup was created",
            text: moment.utc(this.backupInfo.LastBackup).local().fromNow(),
            iconClass: "icon-check",
            textClass: "text-success"
        }];
    }

    stateDataForHtml(nodeTag: string): iconPlusText[] {
        if (this.disabled) {
            if (!nodeTag) {
                return [{
                    text: "Disabled",
                    iconClass: "icon-database-cutout icon-addon-cancel",
                    textClass: "text-danger"
                }];
            }
        } else if (nodeTag) {
            if (!this.online) {
                return [{
                    text: "Offline",
                    iconClass: "icon-database-cutout icon-addon-clock",
                    textClass: "text-warning"
                }];
           } else {
                return [{
                    text: "Online",
                    iconClass: "icon-database-cutout icon-addon-check",
                    textClass: "text-success"
                }];
            }
        }

        return [];
    }
}

export = databaseOverviewItem;
