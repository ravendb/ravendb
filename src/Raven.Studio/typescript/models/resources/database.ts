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
    hasExpirationConfiguration = ko.observable<boolean>(false);
    isEncrypted = ko.observable<boolean>(false);
    
    environment = ko.observable<Raven.Client.Documents.Operations.Configuration.StudioConfiguration.StudioEnvironment>();
    environmentClass = database.createEnvironmentColorComputed("label", this.environment);

    private clusterNodeTag: KnockoutObservable<string>;

    constructor(dbInfo: Raven.Client.ServerWide.Operations.DatabaseInfo, clusterNodeTag: KnockoutObservable<string>) {
        this.clusterNodeTag = clusterNodeTag;

        this.updateUsing(dbInfo);
    }
    
    static createEnvironmentColorComputed(prefix: string, source: KnockoutObservable<Raven.Client.Documents.Operations.Configuration.StudioConfiguration.StudioEnvironment>) {
        return ko.pureComputed(() => {
            const env = source();
            if (env) {
                switch (env) {
                    case "Production":
                        return prefix + "-danger";
                    case "Testing":
                        return prefix + "-success";
                    case "Development":
                        return prefix + "-info";
                }
            }

            return null;
        });
    }

    updateUsing(incomingCopy: Raven.Client.ServerWide.Operations.DatabaseInfo) {
        this.isEncrypted(incomingCopy.IsEncrypted);
        this.hasRevisionsConfiguration(incomingCopy.HasRevisionsConfiguration);
        this.hasExpirationConfiguration(incomingCopy.HasExpirationConfiguration);
        this.isAdminCurrentTenant(incomingCopy.IsAdmin);
        this.name = incomingCopy.Name;
        this.disabled(incomingCopy.Disabled);
        this.environment(incomingCopy.Environment !== "None" ? incomingCopy.Environment : null);
        if (!!incomingCopy.LoadError) {
            this.errored(true);
        }
        
        if (incomingCopy.NodesTopology) {
            const nodeTag = this.clusterNodeTag();
            const inMemberList = _.some(incomingCopy.NodesTopology.Members, x => x.NodeTag === nodeTag);
            const inPromotableList = _.some(incomingCopy.NodesTopology.Promotables, x => x.NodeTag === nodeTag);
            const inRehabList = _.some(incomingCopy.NodesTopology.Rehabs, x => x.NodeTag === nodeTag);

            this.relevant(inMemberList || inPromotableList || inRehabList);
        }
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
