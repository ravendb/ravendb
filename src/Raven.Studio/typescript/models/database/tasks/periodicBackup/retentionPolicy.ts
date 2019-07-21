import jsonUtil = require("common/jsonUtil");
import generalUtils = require("common/generalUtils");

class retentionPolicy {
    disabled = ko.observable<boolean>(false);
    minimumBackupAgeToKeep = ko.observable<number>();

    retentionPolicyEnabled = ko.observable<boolean>(false);
    humaneRetentionDescription: KnockoutComputed<string>;
    
    dirtyFlag: () => DirtyFlag;
    
    constructor(dto: Raven.Client.Documents.Operations.Backups.RetentionPolicy) {
        this.disabled(dto.Disabled);
        this.minimumBackupAgeToKeep(dto.MinimumBackupAgeToKeep ? generalUtils.timeSpanToSeconds(dto.MinimumBackupAgeToKeep) : 0);
        
        this.initObservables();
        this.initValidation();
    }

    private initObservables() {
        this.retentionPolicyEnabled(!this.disabled());
        
        this.humaneRetentionDescription = ko.pureComputed(() => {
            if (this.minimumBackupAgeToKeep() === 0) {
                return "No backups will be removed.";
            }

            const retentionTimeHumane = generalUtils.formatTimeSpan(this.minimumBackupAgeToKeep() * 1000, true);
            return `Bacukups that are older than <strong>${retentionTimeHumane}</strong> will be removed on the next scheduled backup task`;
        });

        this.dirtyFlag = new ko.DirtyFlag([
            this.retentionPolicyEnabled,
            this.minimumBackupAgeToKeep,
        ], false, jsonUtil.newLineNormalizingHashFunction);
    }
    
    private initValidation() {
        this.minimumBackupAgeToKeep.extend({
            validation: [{
                validator: (val: number) => val >= 86400, // 1 day === 86400 seconds
                message: "Retention period must be greater than 1 day"
            }]
        });
    }
    
    toDto(): Raven.Client.Documents.Operations.Backups.RetentionPolicy {
        return {
            Disabled: !this.retentionPolicyEnabled(),
            MinimumBackupAgeToKeep: this.minimumBackupAgeToKeep() === 0 ? null : generalUtils.formatAsTimeSpan(this.minimumBackupAgeToKeep() * 1000)
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
