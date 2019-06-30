import setupEncryptionKey = require("viewmodels/resources/setupEncryptionKey");
import jsonUtil = require("common/jsonUtil");

class retentionPolicy {
    disabled = ko.observable<boolean>(false);
    minimumBackupAgeToKeep = ko.observable<string>();
    minimumBackupsToKeep = ko.observable<number>();
    
    dirtyFlag: () => DirtyFlag;
    
    validationGroup: KnockoutValidationGroup;
    
    constructor(dto: Raven.Client.Documents.Operations.Backups.RetentionPolicy) {
        this.disabled(dto.Disabled);
        this.minimumBackupAgeToKeep(dto.MinimumBackupAgeToKeep);
        this.minimumBackupsToKeep(dto.MinimumBackupsToKeep);
        
        //this.initObservables();
        
        this.dirtyFlag = new ko.DirtyFlag([
            this.disabled,
            this.minimumBackupAgeToKeep,
            this.minimumBackupsToKeep
        ], false, jsonUtil.newLineNormalizingHashFunction);
    }

    toDto(): Raven.Client.Documents.Operations.Backups.RetentionPolicy {
        return {
            Disabled: this.disabled(),
            MinimumBackupAgeToKeep: this.minimumBackupAgeToKeep(),
            MinimumBackupsToKeep: this.minimumBackupsToKeep()
        }
    }

    static empty(): retentionPolicy {
        return new retentionPolicy({
            Disabled: true,
            MinimumBackupAgeToKeep: null,
            MinimumBackupsToKeep: null
        });
    }
}

export = retentionPolicy;
