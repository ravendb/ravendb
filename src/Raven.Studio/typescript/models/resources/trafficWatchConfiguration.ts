/// <reference path="../../../typings/tsd.d.ts"/>

import TrafficWatchChangeType = Raven.Client.Documents.Changes.TrafficWatchChangeType;

class trafficWatchConfiguration {
    
    enabled = ko.observable<boolean>();
    persist = ko.observable<boolean>();

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

    filterCertificateThumbprints = ko.observable<boolean>();
    certificateThumbprints = ko.observableArray<string>();

    certificateThumbprintInput = ko.observable<string>();
    
    validationGroup: KnockoutValidationGroup = ko.validatedObservable({
        databases: this.databases,
        statusCodes: this.statusCodes,
        httpMethods: this.httpMethods,
        changeTypes: this.changeTypes,
        certificateThumbprints: this.certificateThumbprints
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

        this.filterCertificateThumbprints(dto.CertificateThumbprints?.length > 0);
        this.certificateThumbprints(dto.CertificateThumbprints ?? []);
        
        this.initValidation();
        
        _.bindAll(this, "removeCertificateThumbprint", "addCertificateThumbprint");
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

        this.certificateThumbprints.extend({
            required: {
                onlyIf: () => this.filterCertificateThumbprints()
            }
        });
    }

    addCertificateThumbprint() {
        const value = this.certificateThumbprintInput();
        if (!value) {
            return;
        }
        this.certificateThumbprints.push(value);
        this.certificateThumbprintInput("");
    }

    removeCertificateThumbprint(thumbprint: string) {
        this.certificateThumbprints.remove(thumbprint);
    }
    
    toDto(): Raven.Client.ServerWide.Operations.TrafficWatch.PutTrafficWatchConfigurationOperation.Parameters {
        return {
            TrafficWatchMode: this.enabled() ? "ToLogFile" : "Off",
            Databases: this.filterDatabases() ? this.databases() : null,
            StatusCodes: this.filterStatusCodes() ? this.statusCodes() : null,
            MinimumRequestSizeInBytes: this.minimumRequestSize(),
            MinimumResponseSizeInBytes: this.minimumResponseSize(),
            MinimumDurationInMs: this.minimumDuration(),
            HttpMethods: this.filterHttpMethods() ? this.httpMethods() : null,
            ChangeTypes: this.filterChangeTypes() ? this.changeTypes() : null,
            CertificateThumbprints: this.filterCertificateThumbprints() ? this.certificateThumbprints() : null,
            Persist: this.persist(),
        }
    }
}

export = trafficWatchConfiguration;
