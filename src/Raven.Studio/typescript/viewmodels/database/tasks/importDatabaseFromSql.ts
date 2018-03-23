import viewModelBase = require("viewmodels/viewModelBase");
import sqlMigration = require("models/database/tasks/sql/sqlMigration");
import fetchSqlDatabaseSchemaCommand = require("commands/database/tasks/fetchSqlDatabaseSchemaCommand");
import migrateSqlDatabaseCommand = require("commands/database/tasks/migrateSqlDatabaseCommand");
import sqlReference = require("models/database/tasks/sql/sqlReference");
import messagePublisher = require("common/messagePublisher");

//TODO: consider removing 'Please provide 'Database name' in field below, instead of using' - instead automatically extract this from connection string on blur
class importCollectionFromSql extends viewModelBase {
    
    spinners = {
        schema: ko.observable<boolean>(false)
    };
    
    model = new sqlMigration();
    
    inFirstStep = ko.observable<boolean>(true);
    
    databases = ko.observableArray<string>([]); //TODO: fetch this on databases focus
    
    validationGroup: KnockoutValidationGroup;    

    constructor() {
        super();

        this.bindToCurrentInstance("onActionClicked");        
    }
    
    nextStep() {        
        if (!this.isValid(this.model.getValidationGroup())) {
            return false;
        }
        
        const connectionString = this.model.getConnectionString(); 
        console.log("using connection string:" + connectionString);
        console.log("using driver = " + this.model.databaseType());
        
        this.spinners.schema(true);
        
        const schemaRequestDto = {
            Provider: this.model.databaseType(),
            ConnectionString: connectionString
        } as Raven.Server.SqlMigration.Model.SourceSqlDatabase;
        
        new fetchSqlDatabaseSchemaCommand(this.activeDatabase(), schemaRequestDto)
            .execute()
            .done(schema => {
                this.inFirstStep(false);
                this.model.onSchemaUpdated(schema);
            })
            .always(() => this.spinners.schema(false));
            
        //TODO: finish
    }
    
    migrate() {
        const dto = this.model.toDto();
        new migrateSqlDatabaseCommand(this.activeDatabase(), dto)
            .execute()
            .done(() => messagePublisher.reportSuccess("OK!"));
        //TODO: operation id + watch
    }
    
    onActionClicked(reference: sqlReference, action: sqlMigrationAction) {
        reference.action(action);
    }
}

export = importCollectionFromSql; 
