class mountPointUsage {
    freeSpace = ko.observable<number>();
    isLowSpace = ko.observable<boolean>(false);
    mountPoint = ko.observable<string>();
    ravenSize = ko.observable<number>();
    totalCapacity = ko.observable<number>();
    volumeLabel = ko.observable<string>();

    usedSpace: KnockoutComputed<number>;
    usedSpacePercentage: KnockoutComputed<number>;
    ravendbToUsedSpacePercentage: KnockoutComputed<number>;

    mountPointLabel: KnockoutComputed<string>;

    constructor() {
        this.mountPointLabel = ko.pureComputed(() => {
            let mountPoint = this.mountPoint();
            const mountPointLabel = this.volumeLabel();
            if (mountPointLabel) {
                mountPoint += ` (${mountPointLabel})`;
            }

            return mountPoint;
        });

        this.usedSpace = ko.pureComputed(() => {
            const total = this.totalCapacity();
            const free = this.freeSpace();
            return total - free;
        });

        this.ravendbToUsedSpacePercentage = ko.pureComputed(() => {
            const totalUsed = this.usedSpace();
            const documentsUsed = this.ravenSize();

            if (!totalUsed) {
                return 0;
            }

            return documentsUsed *  100.0 / totalUsed;
        });

        this.usedSpacePercentage = ko.pureComputed(() => {
            const total = this.totalCapacity();
            const used = this.usedSpace();

            if (!total) {
                return 0;
            }

            return used * 100.0 / total;
        });
    }

    update(data: Raven.Server.Dashboard.MountPointUsage) {
        this.freeSpace(data.FreeSpace);
        this.isLowSpace(data.IsLowSpace);
        this.mountPoint(data.MountPoint);
        this.ravenSize(data.RavenSize);
        this.totalCapacity(data.TotalCapacity);
        this.volumeLabel(data.VolumeLabel);
    }
}

export = mountPointUsage;
