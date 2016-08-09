class license {
    static licenseStatus = ko.observable<licenseStatusDto>();
    static supportCoverage = ko.observable<supportCoverageDto>();
    static hotSpare = ko.observable<HotSpareDto>();

    static isHotSpare = ko.computed(() => {
        var hotSpare = license.hotSpare();
        if (!!hotSpare) {
            return true;
        }

        var status = license.licenseStatus();
        if (status == null || !status.IsCommercial) {
            return false;
        }

        return status.Attributes.hotSpare === "true";
    });

    static licenseCssClass = ko.computed(() => {
        var status = license.licenseStatus();
        var hotSpare = license.hotSpare();
        if (!!hotSpare) {
            return 'hot-spare';
        }
        if (status == null || !status.IsCommercial) {
            return 'dev-only';
        }
        if (status.Status.contains("Expired")) {
            return 'expired';
        } else {
            return 'commercial';
        }
    });

    static supportCssClass = ko.computed(() => {
        var support = license.supportCoverage();
        if (support == null) {
            return 'no-support';
        }
        switch (support.Status) {
            case 'ProductionSupport':
                return 'production-support';
            case 'ProfessionalSupport':
                return 'professional-support';
            case 'PartialSupport':
                return 'partial-support';
            default:
                return 'no-support';
        }
    });
}

export = license;
