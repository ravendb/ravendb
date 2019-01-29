/// <reference path="../../../../typings/tsd.d.ts"/>

abstract class connectionStringModel {

    isNew = ko.observable<boolean>();
    connectionStringName = ko.observable<string>();   
    
    tasksThatAreUsingThisConnectionString = ko.observableArray<{ taskName: string; taskId: number }>([]);

    constructor(isNew: boolean, tasks: { taskName: string; taskId: number }[]) {      
        this.isNew(isNew);
        this.tasksThatAreUsingThisConnectionString(tasks);
    }    

    update(dto: Raven.Client.Documents.Operations.ConnectionStrings.ConnectionString) {
        this.connectionStringName(dto.Name);        
    }

    initValidation() {
        this.connectionStringName.extend({
            required: true
        });
    }

    setNameUniquenessValidator(validator: (name: string) => boolean) {
        this.connectionStringName.extend({
            validation: [
                {
                    validator: (name: string) => validator(name),
                    message: "Connection string name is already used"
                }
            ]
        })
    }
}

export = connectionStringModel;
