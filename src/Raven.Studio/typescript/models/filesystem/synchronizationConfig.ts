/// <reference path="../../../typings/tsd.d.ts"/>

class synchronizationConfig {

    fileConflictResolution = ko.observable<string>();
    maxNumberOfSynchronizationsPerDestination = ko.observable<number>();
    synchronizationLockTimeout = ko.observable();
    synchronizationLockTimeoutUnit = ko.observable();

    static TU_MINUTES = "minutes";
    static TU_HOURS = "hours";
    static TU_DAYS = "days";

    availableIntervalUnits = [synchronizationConfig.TU_MINUTES, synchronizationConfig.TU_HOURS, synchronizationConfig.TU_DAYS];

    constructor(dto: synchronizationConfigDto) {
        this.fileConflictResolution(dto.FileConflictResolution);
        this.maxNumberOfSynchronizationsPerDestination(dto.MaxNumberOfSynchronizationsPerDestination);
        var timeout = this.prepareInterval(dto.SynchronizationLockTimeoutMiliseconds);
        this.synchronizationLockTimeout(timeout[0]);
        this.synchronizationLockTimeoutUnit(timeout[1]);
    }

    static empty(): synchronizationConfig {
        return new synchronizationConfig({
            FileConflictResolution: "None",
            MaxNumberOfSynchronizationsPerDestination: 5,
            SynchronizationLockTimeoutMiliseconds: 10 * 60 * 1000
        });
    }

    toDto(): synchronizationConfigDto {
        return {
            FileConflictResolution: this.fileConflictResolution(),
            MaxNumberOfSynchronizationsPerDestination: this.maxNumberOfSynchronizationsPerDestination(),
            SynchronizationLockTimeoutMiliseconds: this.convertToMilliseconds(this.synchronizationLockTimeout(), this.synchronizationLockTimeoutUnit())
        };
    }

    private convertToMilliseconds(value, unit): number {
        if (value && unit) {
            switch (unit) {
                case synchronizationConfig.TU_MINUTES:
                    return value * 1000 * 60;
                case synchronizationConfig.TU_HOURS:
                    return value * 1000 * 60 * 60;
                case synchronizationConfig.TU_DAYS:
                    return value * 1000 * 60 * 60 * 24;
            }
        }
        return null;
    }

    private prepareInterval(milliseconds) {
        if (milliseconds) {
            var seconds = milliseconds / 1000;
            var minutes = seconds / 60;
            var hours = minutes / 60;
            if (this.isValidTimeValue(hours)) {
                var days = hours / 24;
                if (this.isValidTimeValue(days)) {
                    return [days, synchronizationConfig.TU_DAYS];
                }
                return [hours, synchronizationConfig.TU_HOURS];
            }
            return [minutes, synchronizationConfig.TU_MINUTES];
        }
        return [0, synchronizationConfig.TU_MINUTES];
    }

    private isValidTimeValue(value: number): boolean {
        return value >= 1 && value % 1 === 0;
    }
}

export = synchronizationConfig;
