/// <reference path="../../../typings/tsd.d.ts"/>
class database {
    static readonly type = "database";
    static readonly qualifier = "db";

    name: string;

    disabled = ko.observable<boolean>(false);
    errored = ko.observable<boolean>(false);
    isAdminCurrentTenant = ko.observable<boolean>(false);
    relevant = ko.observable<boolean>(true);
    hasRevisionsConfiguration = ko.observable<boolean>(false);

    private clusterNodeTag: KnockoutObservable<string>;


    constructor(dbInfo: Raven.Client.Server.Operations.DatabaseInfo, clusterNodeTag: KnockoutObservable<string>) {
        this.clusterNodeTag = clusterNodeTag;

        this.updateUsing(dbInfo);
        /* TODO
        this.isLicensed = ko.pureComputed(() => {
            if (!!license.licenseStatus() && license.licenseStatus().IsCommercial) {
                var attributes = license.licenseStatus().Attributes;
                var result = this.activeBundles()
                    .map(bundleName => this.attributeValue(attributes, bundleName))
                    .reduce((a, b) => /^true$/i.test(a) && /^true$/i.test(b), true);
                return result;
            }
            return true;
        });*/
    }

    updateUsing(incomingCopy: Raven.Client.Server.Operations.DatabaseInfo) {
        this.hasRevisionsConfiguration(incomingCopy.HasRevisionsConfiguration);
        this.isAdminCurrentTenant(incomingCopy.IsAdmin);
        this.name = incomingCopy.Name;
        this.disabled(incomingCopy.Disabled);
        this.errored(!!incomingCopy.LoadError);

        const nodeTag = this.clusterNodeTag();
        const inMemberList = _.some(incomingCopy.NodesTopology.Members, x => x.NodeTag === nodeTag);
        const inPromotableList = _.some(incomingCopy.NodesTopology.Promotables, x => x.NodeTag === nodeTag);

        this.relevant(inMemberList || inPromotableList);
    }

    private attributeValue(attributes: any, bundleName: string) {
        for (var key in attributes){
            if (attributes.hasOwnProperty(key) && key.toLowerCase() === bundleName.toLowerCase()) {
                return attributes[key];
            }
        }
        return "true";
    }

    static getNameFromUrl(url: string) {
        var index = url.indexOf("databases/");
        return (index > 0) ? url.substring(index + 10) : "";
    }

    //TODO: remove those props?
    get fullTypeName() {
        return "Database";
    }

    get qualifier() {
        return database.qualifier;
    }

    get urlPrefix() {
        return "databases";
    }

    get type() {
        return database.type;
    }
}

export = database;
