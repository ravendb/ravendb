/// <reference path="../../../../typings/tsd.d.ts"/>

import resourceInfo = require("models/resources/info/resourceInfo");
import databaseInfo = require("models/resources/info/databaseInfo");

class resourcesInfo {

    sortedResources = ko.observableArray<resourceInfo>();

    databasesCount: KnockoutComputed<number>;


    constructor(dto: Raven.Client.Server.Operations.ResourcesInfo) {

        const databases = dto.Databases.map(db => new databaseInfo(db));

        const resources = [...databases] as resourceInfo[];
        resources.sort((a, b) => a.name.toLowerCase().localeCompare(b.name.toLowerCase()));

        this.sortedResources(resources);

        this.initObservables();
    }

    getByQualifiedName(qualifiedName: string) {
        return this.sortedResources().find(x => x.qualifiedName.toLowerCase() === qualifiedName.toLowerCase());
    }

    updateResource(newResourceInfo: Raven.Client.Server.Operations.ResourceInfo, resourceType: string) {
        let resourceToUpdate = this.getByQualifiedName(resourceType + "/" + newResourceInfo.Name);

        if (resourceToUpdate) {
            resourceToUpdate.update(newResourceInfo);
        } else { // new resource - create instance of it
            let resourceToAdd: resourceInfo;
            switch (resourceType) { //TODO: no need
                case "db":

                    let dto = newResourceInfo as Raven.Client.Server.Operations.DatabaseInfo;
                    resourceToAdd = new databaseInfo(dto);
                    break;

                default:
                    throw new Error("Unsupported resource type = " + resourceType);
            }

            let locationToInsert = _.sortedIndexBy(this.sortedResources(), resourceToAdd, function (item) { return item.name.toLowerCase() });
            this.sortedResources.splice(locationToInsert, 0, resourceToAdd);
        }
    }

    private initObservables() {
        this.databasesCount = ko.pureComputed(() => this
            .sortedResources()
            .filter(r => r instanceof databaseInfo)
            .length);
    }
}

export = resourcesInfo;
