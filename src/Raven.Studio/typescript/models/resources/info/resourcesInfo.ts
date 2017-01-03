/// <reference path="../../../../typings/tsd.d.ts"/>

import resourceInfo = require("models/resources/info/resourceInfo");
import databaseInfo = require("models/resources/info/databaseInfo");
import filesystemInfo = require("models/resources/info/filesystemInfo");

class resourcesInfo {

    sortedResources = ko.observableArray<resourceInfo>();

    databasesCount: KnockoutComputed<number>;
    filesystemCount: KnockoutComputed<number>;


    constructor(dto: Raven.Client.Data.ResourcesInfo) {

        const databases = dto.Databases.map(db => new databaseInfo(db));
        //TODO: fs, cs, ts

        const resources = [...databases] as resourceInfo[];
        resources.sort((a, b) => a.name.toLowerCase().localeCompare(b.name.toLowerCase()));

        this.sortedResources(resources);

        this.initObservables();
    }

    getByQualifiedName(qualifiedName: string) {
        return this.sortedResources().find(x => x.qualifiedName.toLowerCase() === qualifiedName.toLowerCase());
    }

    updateResource(newResourceInfo: Raven.Client.Data.ResourceInfo, resourceType: string) {
        let resourceToUpdate = this.getByQualifiedName(resourceType + "/" + newResourceInfo.Name);

        if (resourceToUpdate) {
            resourceToUpdate.update(newResourceInfo);
        } else { // new resource - create instance of it
            let resourceToAdd: resourceInfo;
            switch (resourceType) {
                case "db":

                    let dto = newResourceInfo as Raven.Client.Data.DatabaseInfo;
                    resourceToAdd = new databaseInfo(dto);
                    break;

                default:
                    throw new Error("Unsupported resource type = " + resourceType);

                //TODO: implement fs, cs, ts		
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

        this.filesystemCount = ko.pureComputed(() => this
            .sortedResources()
            .filter(r => r instanceof filesystemInfo)
            .length);
    }
}

export = resourcesInfo;
