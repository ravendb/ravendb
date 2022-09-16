/// <reference path="../../../typings/tsd.d.ts"/>

import TrafficWatchChangeType = Raven.Client.Documents.Changes.TrafficWatchChangeType;

class trafficWatchConfiguration {
    
    enabled = ko.observable<boolean>();

    filterDatabases = ko.observable<boolean>();
    databases = ko.observableArray<string>();

    filterStatusCodes = ko.observable<boolean>();
    statusCodes = ko.observableArray<number>();

    minimumResponseSize = ko.observable<number>();
    minimumRequestSize = ko.observable<number>();
    minimumDuration = ko.observable<number>();

    filterHttpMethods = ko.observable<boolean>();
    httpMethods = ko.observableArray<string>([]);

    filterChangeTypes = ko.observable<boolean>();
    changeTypes = ko.observableArray<TrafficWatchChangeType>([]);
    
    validationGroup: KnockoutValidationGroup = ko.validatedObservable({
        databases: this.databases,
        statusCodes: this.statusCodes,
        httpMethods: this.httpMethods,
        changeTypes: this.changeTypes
    })
    
    constructor(dto: Raven.Client.ServerWide.Operations.TrafficWatch.PutTrafficWatchConfigurationOperation.Parameters) {
        this.enabled(dto.TrafficWatchMode === "ToLogFile");
        
        this.filterDatabases(dto.Databases?.length > 0);
        this.databases(dto.Databases ?? []);
        
        this.filterStatusCodes(dto.StatusCodes?.length > 0);
        this.statusCodes(dto.StatusCodes ?? []);
        
        this.minimumRequestSize(dto.MinimumRequestSizeInBytes);
        this.minimumResponseSize(dto.MinimumResponseSizeInBytes);
        this.minimumDuration(dto.MinimumDurationInMs);
        
        this.filterHttpMethods(dto.HttpMethods?.length > 0);
        this.httpMethods(dto.HttpMethods ?? []);
        
        this.filterChangeTypes(dto.ChangeTypes?.length > 0);
        this.changeTypes(dto.ChangeTypes ?? []);
        
        this.initValidation();
    }
    
    private initValidation() {
        this.databases.extend({
            required: {
                onlyIf: () => this.filterDatabases()
            }
        });
        
        this.statusCodes.extend({
            required: {
                onlyIf: () => this.filterStatusCodes()
            }
        });
        
        this.httpMethods.extend({
            required: {
                onlyIf: () => this.filterHttpMethods()
            }
        });
        
        this.changeTypes.extend({
            required: {
                onlyIf: () => this.filterChangeTypes()
            }
        });
    }
    
    
    toDto(): Raven.Client.ServerWide.Operations.TrafficWatch.PutTrafficWatchConfigurationOperation.Parameters {
        return {
            TrafficWatchMode: this.enabled() ? "ToLogFile" : "Off",
            Databases: this.filterDatabases() ? this.databases() : null,
            StatusCodes: this.filterStatusCodes() ? this.statusCodes() : null,
            MinimumRequestSizeInBytes: this.minimumRequestSize(),
            MinimumResponseSizeInBytes: this.minimumRequestSize(),
            MinimumDurationInMs: this.minimumDuration(),
            HttpMethods: this.filterHttpMethods() ? this.httpMethods() : null,
            ChangeTypes: this.filterChangeTypes() ? this.changeTypes() : null
        }
    }
}

export = trafficWatchConfiguration;
