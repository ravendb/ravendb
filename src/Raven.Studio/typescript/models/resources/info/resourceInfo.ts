/// <reference path="../../../../typings/tsd.d.ts"/>

import resource = require("models/resources/resource");
import generalUtils = require("common/generalUtils");
import activeResourceTracker = require("common/shell/activeResourceTracker");

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
    lastBackup = ko.observable<string>();
    backupStatus = ko.observable<string>();
    backupEnabled = ko.observable<boolean>();

    filteredOut = ko.observable<boolean>(false);

    badgeClass: KnockoutComputed<string>;
    badgeText: KnockoutComputed<string>;
    online: KnockoutComputed<boolean>;

    canNavigateToResource: KnockoutComputed<boolean>;

    protected constructor(dto: Raven.Client.Data.ResourceInfo) {
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
            this.lastBackup(generalUtils.timeSpanAsAgo(dto.BackupInfo.LastBackup, true));
            this.backupStatus(this.computeBackupStatus(dto.BackupInfo));
        }
        this.initializeObservables();
    }

    abstract get qualifier(): string;

    abstract get fullTypeName(): string;

    get qualifiedName() {
        return this.qualifier + "/" + this.name;
    }

    abstract asResource(): resource;

    private computeBackupStatus(dto: Raven.Client.Data.BackupInfo) {
        if (!dto.LastBackup) {
            return "text-danger";
        }
        const interval = moment.duration(dto.BackupInterval).asSeconds();
        const lastBackup = moment.duration(dto.LastBackup).asSeconds();

        return (interval * 1.2 < lastBackup) ? "text-warning" : "text-success";
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
            return ""; // offline
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
    }

}

export = resourceInfo;
