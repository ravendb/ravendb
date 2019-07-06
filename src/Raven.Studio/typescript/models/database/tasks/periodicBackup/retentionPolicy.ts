import setupEncryptionKey = require("viewmodels/resources/setupEncryptionKey");
import jsonUtil = require("common/jsonUtil");

class retentionPolicy {
    disabled = ko.observable<boolean>(false);
    minimumBackupAgeToKeep = ko.observable<string>();
    
    dirtyFlag: () => DirtyFlag;
    
    validationGroup: KnockoutValidationGroup;
    
    constructor(dto: Raven.Client.Documents.Operations.Backups.RetentionPolicy) {
        this.disabled(dto.Disabled);
        this.minimumBackupAgeToKeep(dto.MinimumBackupAgeToKeep);
        
        this.dirtyFlag = new ko.DirtyFlag([
            this.disabled,
            this.minimumBackupAgeToKeep,
        ], false, jsonUtil.newLineNormalizingHashFunction);
    }

    toDto(): Raven.Client.Documents.Operations.Backups.RetentionPolicy {
        return {
            Disabled: this.disabled(),
            MinimumBackupAgeToKeep: this.minimumBackupAgeToKeep()
        }
    }

    static empty(): retentionPolicy {
        return new retentionPolicy({
            Disabled: true,
            MinimumBackupAgeToKeep: null
        });
    }
}

export = retentionPolicy;
