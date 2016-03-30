import dialog = require("plugins/dialog");
import dialogViewModelBase = require("viewmodels/dialogViewModelBase");

type attributeItem = {
    displayName: string;
    value: string;
};

class licensingStatus extends dialogViewModelBase {

    isDevelopmentOnly: boolean;
    isNonExpiredCommercial: boolean;
    isExpired: boolean;
    licenseStatusText: string;

    supportStatus: string;
    isCommunitySupport: boolean;
    isProfessionalSupport: boolean;
    isProductionSupport: boolean;

    attrRam: string;
    attrCpus: string;
    attributes: attributeItem[];

    constructor(private licenseStatus: licenseStatusDto, supportCoverage: supportCoverageDto) {
        super();

        this.isDevelopmentOnly = !licenseStatus.IsCommercial;
        this.isNonExpiredCommercial = licenseStatus.IsCommercial && !licenseStatus.Status.contains("Expired");
        this.isExpired = licenseStatus.IsCommercial && licenseStatus.Status.contains("Expired");
        this.licenseStatusText = licenseStatus.Status;
        this.supportStatus = supportCoverage.Status;
        this.isCommunitySupport = supportCoverage.Status !== 'ProductionSupport' && supportCoverage.Status !== 'ProfessionalSupport';
        this.isProfessionalSupport = supportCoverage.Status === 'ProfessionalSupport';
        this.isProductionSupport = supportCoverage.Status === 'ProductionSupport';

        this.attrRam = this.prepareHtmlForAttribute(licenseStatus.Attributes.maxRamUtilization);
        this.attrCpus = this.prepareHtmlForAttribute(licenseStatus.Attributes.maxParallelism);

        this.attributes = [
            { displayName: "Databases", value: licenseStatus.Attributes.numberOfDatabases },
            { displayName: "Database size", value: licenseStatus.Attributes.maxSizeInMb },
            { displayName: "RavenFS", value: licenseStatus.Attributes.ravenfs },
            { displayName: "Periodic Backup", value: licenseStatus.Attributes.periodicBackup },
            { displayName: "Encryption", value: licenseStatus.Attributes.encryption },
            { displayName: "FIPS Compliance", value: licenseStatus.Attributes.fips },
            { displayName: "Global Configuration", value: licenseStatus.Attributes.globalConfiguration },
            { displayName: "Compression", value: licenseStatus.Attributes.compression },
            { displayName: "Quotas", value: licenseStatus.Attributes.quotas },
            { displayName: "Counters", value: licenseStatus.Attributes.counters },
            { displayName: "Time Series", value: licenseStatus.Attributes.timeSeries },
            { displayName: "Authorization", value: licenseStatus.Attributes.authorization },
            { displayName: "Document Expiration", value: licenseStatus.Attributes.documentExpiration },
            { displayName: "Replication", value: licenseStatus.Attributes.replication },
            { displayName: "Versioning", value: licenseStatus.Attributes.versioning },
            { displayName: "Cluster", value: licenseStatus.Attributes.cluster },
            { displayName: "Monitoring", value: licenseStatus.Attributes.monitoring },
            { displayName: "Hot Spare", value: licenseStatus.Attributes.hotSpare },
            { displayName: "Allow Windows Clustering", value: licenseStatus.Attributes.allowWindowsClustering }
        ];

        this.attributes.forEach(attr => attr.value = this.prepareHtmlForAttribute(attr.value));
    }

    private prepareHtmlForAttribute(input: string) {
        if (input === 'unlimited') {
            return '<i class="fa fa-infinity"></i>';
        } else if (input === 'true') {
            return '<i class="fa fa-check"></i>';
        } else if (input === 'false' || !input) {
            return '<i class="fa fa-times"></i>';
        } 
        return input;
    }

    cancel() {
        dialog.close(this);
    }

    ok() {
        dialog.close(this);
    }

}

export = licensingStatus;
