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
