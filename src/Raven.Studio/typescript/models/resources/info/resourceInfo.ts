/// <reference path="../../../../typings/tsd.d.ts"/>

import resource = require("models/resources/resource");
import generalUtils = require("common/generalUtils");
import activeResourceTracker = require("common/shell/activeResourceTracker");
import getResourceCommand = require("commands/resources/getResourceCommand");

abstract class resourceInfo {

    name: string;
    bundles = ko.observableArray<string>();
    isAdmin = ko.observable<boolean>();
    isCurrentlyActiveResource: KnockoutComputed<boolean>;
    disabled = ko.observable<boolean>();
    totalSize = ko.observable<string>();

    errors = ko.observable<number>();
    alerts = ko.observable<number>();

    hasErrors: KnockoutComputed<boolean>;
    hasAlerts: KnockoutComputed<boolean>;

    licensed = ko.observable<boolean>(true); //TODO: bind this value

    uptime = ko.observable<string>();
    lastFullOrIncrementalBackup = ko.observable<string>();
    backupStatus = ko.observable<string>();
    backupEnabled = ko.observable<boolean>();

    filteredOut = ko.observable<boolean>(false);

    badgeClass: KnockoutComputed<string>;
    badgeText: KnockoutComputed<string>;
    online: KnockoutComputed<boolean>;
    isLoading: KnockoutComputed<boolean>;

    canNavigateToResource: KnockoutComputed<boolean>;

    static extractQualifierAndNameFromNotification(input: string): { qualifier: string, name: string } {
        return { qualifier: input.substr(0, 2), name: input.substr(3) };
    }

    protected constructor(dto: Raven.Client.Data.ResourceInfo) {
        this.initializeObservables();
    }

    abstract get qualifier(): string;

    abstract get fullTypeName(): string;

    get qualifiedName() {
        return this.qualifier + "/" + this.name;
    }

    static findLastBackupDate(dto: Raven.Client.Data.BackupInfo) {
        const lastFull = dto.LastFullBackup;
        const lastIncrementalBackup = dto.LastIncrementalBackup;

        if (lastFull && lastIncrementalBackup) {
            return lastFull > lastIncrementalBackup ? lastFull : lastIncrementalBackup;
        } else if (lastFull) {
            return lastFull;
        }
        return lastIncrementalBackup;
    }

    abstract asResource(): resource;

    update(dto: Raven.Client.Data.ResourceInfo): void {
        this.name = dto.Name;
        this.disabled(dto.Disabled);
        this.isAdmin(dto.IsAdmin);
        this.totalSize(dto.TotalSize ? dto.TotalSize.HumaneSize : null);
        this.errors(dto.Errors);
        this.alerts(dto.Alerts);
        this.bundles(dto.Bundles);
        this.uptime(generalUtils.timeSpanAsAgo(dto.UpTime, false));
        this.backupEnabled(!!dto.BackupInfo);
        if (this.backupEnabled()) {
            const lastBackup = resourceInfo.findLastBackupDate(dto.BackupInfo);
            this.lastFullOrIncrementalBackup(moment(new Date(lastBackup)).fromNow());
            this.backupStatus(this.computeBackupStatus(dto.BackupInfo));
        }
    }

    private computeBackupStatus(dto: Raven.Client.Data.BackupInfo) {
        if (!dto.LastFullBackup && !dto.LastIncrementalBackup) {
            return "text-danger";
        }

        const fullBackupInterval = moment.duration(dto.FullBackupInterval).asSeconds();
        const incrementalBackupInterval = moment.duration(dto.IncrementalBackupInterval).asSeconds();

        const interval = (incrementalBackupInterval === 0) ? fullBackupInterval : Math.min(incrementalBackupInterval, fullBackupInterval);

        const lastBackup = new Date(resourceInfo.findLastBackupDate(dto));

        const secondsSinceLastBackup = moment.duration(moment().diff(moment(lastBackup))).asSeconds();

        return (interval * 1.2 < secondsSinceLastBackup) ? "text-warning" : "text-success";
    }

    private initializeObservables() {
        this.online = ko.pureComputed(() => {
            return !!this.uptime();
        });

        this.badgeClass = ko.pureComputed(() => {
            if (!this.licensed()) {
                return "state-danger";
            }

            if (this.disabled()) {
                return "state-warning";
            }

            if (this.online()) {
                return "state-success";
            }

            return "state-offline"; // offline
        });

        this.badgeText = ko.pureComputed(() => {
            if (!this.licensed()) {
                return "Unlicensed";
            }
            if (this.disabled()) {
                return "Disabled";
            }

            if (this.uptime()) {
                return "Online";
            }
            return "Offline";
        });

        this.canNavigateToResource = ko.pureComputed(() => {
            const hasLicense = this.licensed();
            const enabled = !this.disabled();
            return hasLicense && enabled;
        });

        this.isCurrentlyActiveResource = ko.pureComputed(() => {
            const currentResource = activeResourceTracker.default.resource();

            if (!currentResource) {
                return false;
            }

            return currentResource.qualifiedName === this.qualifiedName;
        });

        this.isLoading = ko.pureComputed(() => {
            return this.isCurrentlyActiveResource() &&
                this.online() === false &&
                this.disabled() === false;
        });
    }
}

export = resourceInfo;
