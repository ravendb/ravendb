import dialogViewModelBase = require("viewmodels/dialogViewModelBase");
import databasesManager = require("common/shell/databasesManager");
import notificationCenter = require("common/notifications/notificationCenter");
import databaseInfo = require("models/resources/info/databaseInfo");
import getIndexNamesCommand = require("commands/database/index/getIndexNamesCommand");
import compactDatabaseCommand = require("commands/resources/compactDatabaseCommand");
import genUtils = require("common/generalUtils");
import dialog = require("plugins/dialog");
import clusterTopologyManager = require("common/shell/clusterTopologyManager");

class compactDatabaseDialog extends dialogViewModelBase {
    
    database: databaseInfo;
    
    allIndexes = ko.observableArray<string>([]);
    indexesToCompact = ko.observableArray<string>([]);
        
    compactDocuments = ko.observable<boolean>(true);
    compactAllIndexes = ko.observable<boolean>();

    filterText = ko.observable<string>();
    filteredIndexes: KnockoutComputed<Array<string>>;

    compactEnabled: KnockoutComputed<boolean>;
    selectAllIndexesEnabled: KnockoutComputed<boolean>;
    
    numberOfIndexesFormatted: KnockoutComputed<string>;
    numberOfSelectedIndexesFormatted: KnockoutComputed<string>;
    
    numberOfNodes: KnockoutComputed<number>;
    currentNodeTag: KnockoutComputed<string>;
    
    constructor(db: databaseInfo) {
        super(); 
        
        this.database = db;
        this.initObservables();
    }

    activate() {
        new getIndexNamesCommand(databasesManager.default.getDatabaseByName(this.database.name))
            .execute()
            .done(indexNames => {
                this.allIndexes(indexNames);
                this.compactAllIndexes(!!indexNames.length);
            });
    }

    protected initObservables() {
        this.numberOfIndexesFormatted = ko.pureComputed(() => {
            return `(${genUtils.formatAsCommaSeperatedString(this.database.indexesCount(), 0)})`;
        })

        this.numberOfSelectedIndexesFormatted = ko.pureComputed(() => {
            return `(${genUtils.formatAsCommaSeperatedString(this.indexesToCompact().length, 0)} selected)`;
        })
        
        this.compactEnabled = ko.pureComputed(() => {
            return this.compactDocuments() || !!this.indexesToCompact().length;
        })
        
        this.filteredIndexes = ko.pureComputed(() => {
            const filter = this.filterText();
            
            if (!this.filterText()) {
                return this.allIndexes();
            }
            
            return this.allIndexes().filter(x => x.toLowerCase().includes(filter.toLowerCase()));
        });

        this.compactAllIndexes.subscribe(compactAll => {
            this.indexesToCompact(compactAll ? [...this.allIndexes()] : []);
        });
        
        this.selectAllIndexesEnabled = ko.pureComputed(() => {
            return !_.isEqual(this.allIndexes(), this.indexesToCompact());
        });
        
        this.numberOfNodes = ko.pureComputed(() => {
            return clusterTopologyManager.default.topology().nodes().length;
        });

        this.currentNodeTag = ko.pureComputed(() => {
            return clusterTopologyManager.default.topology().nodeTag();
        });
    }   
    
    compactDatabase() {
        this.database.inProgressAction("Compacting...");

        new compactDatabaseCommand(this.database.name, this.compactDocuments(), this.indexesToCompact())
            .execute()
            .done(result => {
                notificationCenter.instance.monitorOperation(null, result.OperationId)
                    .always(() => this.database.inProgressAction(null));

                notificationCenter.instance.openDetailsForOperationById(null, result.OperationId);

                dialog.close(this);
            })
            .fail(() => this.database.inProgressAction(null));
    }

    selectAllIndexes() {
        this.indexesToCompact([...this.allIndexes()]);
    }
}

export = compactDatabaseDialog;
