/// <reference path="../../../../typings/tsd.d.ts"/>
import replicationCertificateModel = require("models/database/tasks/replicationCertificateModel");
import prefixPathModel = require("models/database/tasks/prefixPathModel");
import replicationAccessBaseModel = require("models/database/tasks/replicationAccessBaseModel");

class replicationAccessHubModel extends replicationAccessBaseModel {
  
    isNewAccessItem = ko.observable<boolean>();
    
    usingExistingCertificate = ko.observable<boolean>(false); 
    
    accessConfigurationWasExported = ko.observable<boolean>(false);
    certificateWasDownloaded = ko.observable<boolean>(false);
    
    certificateInfoWasSavedForSinkTask: KnockoutComputed<boolean>;

    filteringReplicationText: KnockoutComputed<string>;
    
    disableExport: KnockoutComputed<boolean>;
    disableDownload: KnockoutComputed<boolean>;

    validationGroupForSaveWithFiltering: KnockoutValidationGroup;
    validationGroupForSaveNoFiltering: KnockoutValidationGroup;
    validationGroupForExportWithFiltering: KnockoutValidationGroup;
    validationGroupForExportNoFiltering: KnockoutValidationGroup;

    constructor(accessName: string, certificate: replicationCertificateModel,
                hubToSink: prefixPathModel[], sinkToHub: prefixPathModel[],
                filteringRequired: boolean, isNewItem = true) {
        
        super(accessName, certificate, hubToSink, sinkToHub, filteringRequired);
        
        this.isNewAccessItem(isNewItem);

        this.initObservables();
        this.initValidation();
    }
    
    initObservables() {
        super.initObservables();

        this.certificateInfoWasSavedForSinkTask = ko.pureComputed(() => {
            return this.accessConfigurationWasExported() || this.certificateWasDownloaded();
        })
        
        this.disableExport = ko.pureComputed(() => {
            return !this.certificate() || !this.replicationAccessName() || (!this.hubToSinkPrefixes().length && this.filteringPathsRequired()); 
        })

        this.disableDownload = ko.pureComputed(() => {
            return !this.certificate() || this.usingExistingCertificate();
        })
        
        this.filteringReplicationText = ko.pureComputed(() => {

            const h2s = this.hubToSinkPrefixes().length;
            const s2h = this.sinkToHubPrefixes().length

            let text = "";

            if (h2s) {
                text = `Hub to Sink (${h2s} path${h2s > 1 ? "s" : ""})`
            }

            if (h2s && s2h) {
                text += ", ";
            }
            if (s2h) {
                text += `Sink to Hub (${s2h} path${s2h > 1 ? "s" : ""})`
            }

            return text;
        })
    }
    
    initValidation() {
        super.initValidation();
        
        this.certificateInfoWasSavedForSinkTask.extend({
            validation: [
                {
                    validator: () => !this.isNewAccessItem() ||
                                     this.usingExistingCertificate() ||
                                     !this.certificate() ||
                                     this.certificateInfoWasSavedForSinkTask(),
                    message: "Export the Access Configuration or download the certificate before saving."
                }
            ]
        });

        this.validationGroupForSaveWithFiltering = ko.validatedObservable({
            replicationAccessName: this.replicationAccessName,
            certificate: this.certificate,
            certificateInfoWasSavedForSinkTask: this.certificateInfoWasSavedForSinkTask,
            hubToSinkPrefixes: this.hubToSinkPrefixes,
            sinkToHubPrefixes: this.sinkToHubPrefixes
        });

        this.validationGroupForSaveNoFiltering = ko.validatedObservable({
            replicationAccessName: this.replicationAccessName,
            certificate: this.certificate,
            certificateInfoWasSavedForSinkTask: this.certificateInfoWasSavedForSinkTask,
        });

        this.validationGroupForExportWithFiltering = ko.validatedObservable({
            replicationAccessName: this.replicationAccessName,
            certificate: this.certificate,
            hubToSinkPrefixes: this.hubToSinkPrefixes,
            sinkToHubPrefixes: this.sinkToHubPrefixes
        });

        this.validationGroupForExportNoFiltering = ko.validatedObservable({
            replicationAccessName: this.replicationAccessName,
            certificate: this.certificate
        });
    }
    
    getValidationGroupForExport(withFiltering: boolean) {
        if (withFiltering) {
            return this.validationGroupForExportWithFiltering;
        }
        
        return this.validationGroupForExportNoFiltering;
    }

    getValidationGroupForSave(withFiltering: boolean) {
        if (withFiltering) {
            return this.validationGroupForSaveWithFiltering;
        }

        return this.validationGroupForSaveNoFiltering;
    }

    static empty(filteringRequired: boolean): replicationAccessHubModel {
        return new replicationAccessHubModel("", null, [], [], filteringRequired);
    }
    
    static clone(itemToClone: replicationAccessHubModel): replicationAccessHubModel {
        return new replicationAccessHubModel(
            itemToClone.replicationAccessName(),
            itemToClone.certificate(),
            itemToClone.hubToSinkPrefixes(),
            itemToClone.sinkToHubPrefixes(),
            itemToClone.filteringPathsRequired(),
            itemToClone.isNewAccessItem()
        );
    }
}

export = replicationAccessHubModel;
