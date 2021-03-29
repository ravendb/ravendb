

class cpuUsage {
    readonly tag: string;
    disconnected = ko.observable<boolean>(true);

    hasData = ko.observable<boolean>(false);

    machineCpuUsage = ko.observable<number>(0);
    processCpuUsage = ko.observable<number>(0);

    numberOfCores = ko.observable<number>();
    utilizedCores = ko.observable<number>();

    coresInfo: KnockoutComputed<string>;
    processCpuUsageFormatted: KnockoutComputed<string>;
    machineCpuUsageFormatted: KnockoutComputed<string>;

    constructor(tag: string) {
        this.tag = tag;

        this.coresInfo = ko.pureComputed(() => {
            if (!this.hasData()) {
                return "-/- Cores";
            }
            return this.utilizedCores() + "/" + this.numberOfCores() + " Cores";
        });

        this.processCpuUsageFormatted = ko.pureComputed(() => {
            if (this.disconnected() || !this.hasData()) {
                return "Connecting...";
            }
            return this.processCpuUsage() + "%";
        });

        this.machineCpuUsageFormatted = ko.pureComputed(() => {
            if (this.disconnected() || !this.hasData()) {
                return "Connecting...";
            }
            return this.machineCpuUsage() + "%";
        });
    }

    update(data: Raven.Server.Dashboard.Cluster.Notifications.CpuUsagePayload) {
        this.hasData(true);

        this.machineCpuUsage(data.MachineCpuUsage);
        this.processCpuUsage(data.ProcessCpuUsage);

        this.numberOfCores(data.NumberOfCores);
        this.utilizedCores(data.UtilizedCores);
    }
}


export = cpuUsage;
