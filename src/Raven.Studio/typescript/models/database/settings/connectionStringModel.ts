/// <reference path="../../../../typings/tsd.d.ts"/>

abstract class connectionStringModel {

    isNew = ko.observable<boolean>();
    connectionStringName = ko.observable<string>();   
    tasksThatAreUsingThisConnectionString = ko.observableArray<string>([]);

    constructor(isNew: boolean, tasks: string[]) {      
        this.isNew(isNew);
        this.tasksThatAreUsingThisConnectionString(tasks);
    }    

    update(dto: Raven.Client.ServerWide.ConnectionString) {
        this.connectionStringName(dto.Name);        
    }

    initValidation() {
        this.connectionStringName.extend({
            required: true
        });        
    }   
}

export = connectionStringModel;
