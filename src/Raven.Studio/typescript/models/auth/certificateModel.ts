/// <reference path="../../../typings/tsd.d.ts" />

import certificatePermissionModel = require("models/auth/certificatePermissionModel");

class certificateModel {

    static securityClearanceTypes: valueAndLabelItem<securityClearanceTypes,string>[] = [
        {
            label: "Cluster Administator",
            value: "ClusterAdmin"
        }, {
            label: "Operations", 
            value: "Operations"
        }, {
            label: "User",
            value: "User"
        }];
    
    mode = ko.observable<certificateMode>();
    
    name = ko.observable<string>();
    securityClearance = ko.observable<securityClearanceTypes>("User");
    
    certificateAsBase64 = ko.observable<string>();
    certificatePassphrase = ko.observable<string>();
    expirationDate = ko.observable<string>();
    thumbprint = ko.observable<string>();

    permissions = ko.observableArray<certificatePermissionModel>();

    securityClearanceLabel: KnockoutComputed<string>;
    
    validationGroup = ko.validatedObservable({
        name: this.name,
        certificateAsBase64: this.certificateAsBase64
    });
    
    private constructor(mode: certificateMode) {
        this.mode(mode);

        _.bindAll(this, "setClearanceMode");
        
        this.initObservables();
        this.initValidation();
    }
    
    private initObservables() {
        this.securityClearanceLabel = ko.pureComputed(() => {
            const clearance = this.securityClearance();
            if (!clearance) {
                return "";
            }
            
            return certificateModel.securityClearanceTypes.find(x => x.value === clearance).label;
        })
    }

    private initValidation() {
        this.name.extend({
            required: true
        });
        
        this.certificateAsBase64.extend({
            required: true //TODO: it isn't always required
        });
    }

    setClearanceMode(mode: securityClearanceTypes) {
        this.securityClearance(mode);
    }


    toGenerateCertificateDto() {
        return {
            Name: this.name(),
            Password: this.certificatePassphrase(),
            Permissions: this.serializePermissions()
        }
    }
    
    toUploadCertificateDto() {
        return {
            Name: this.name(),
            Certificate: this.certificateAsBase64(),
            Password: this.certificatePassphrase(),
            Permissions: this.serializePermissions()
            //TODO: other props
        }
    }
    
    private serializePermissions() : dictionary<Raven.Client.ServerWide.Operations.Certificates.DatabaseAccess> {
        return null; //TODO:
    }
    
    static generate() {
        return new certificateModel("generate");
    }
    
    static upload() {
        return new certificateModel("upload");
    }
    
    static fromDto(dto: Raven.Client.ServerWide.Operations.Certificates.CertificateDefinition) {
        const model = new certificateModel("editExisting");
        //TODO: fill properties
        return model;
    }
    
    //TODO: edit existing cert 
    
    
}

export = certificateModel;
