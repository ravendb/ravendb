/// <reference path="../../../typings/tsd.d.ts" />

import certificatePermissionModel = require("models/auth/certificatePermissionModel");

class certificateModel {

    static securityClearanceTypes: valueAndLabelItem<Raven.Client.ServerWide.Operations.Certificates.SecurityClearance, string>[] = [
        {
            label: "Cluster Administator",
            value: "ClusterAdmin"
        }, {
            label: "Operator", 
            value: "Operator"
        }, {
            label: "User",
            value: "ValidUser"
        }];
    
    mode = ko.observable<certificateMode>();
    
    name = ko.observable<string>();
    securityClearance = ko.observable<Raven.Client.ServerWide.Operations.Certificates.SecurityClearance>("ValidUser");
    
    certificateAsBase64 = ko.observable<string>();
    certificatePassphrase = ko.observable<string>();
    expirationDate = ko.observable<string>();
    thumbprint = ko.observable<string>();

    permissions = ko.observableArray<certificatePermissionModel>();

    securityClearanceLabel: KnockoutComputed<string>;
    
    validationGroup: KnockoutValidationGroup = ko.validatedObservable({
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
            
            return certificateModel.clearanceLabelFor(clearance);
        })
    }
    
    static clearanceLabelFor(input: Raven.Client.ServerWide.Operations.Certificates.SecurityClearance) {
        return certificateModel.securityClearanceTypes.find(x => x.value === input).label;
    }

    private initValidation() {
        this.name.extend({
            required: {
                onlyIf: () => this.mode() !== "editExisting"
            }
        });
        
        this.certificateAsBase64.extend({
            required: {
                onlyIf: () => this.mode() === "upload"
            } 
        });
    }

    setClearanceMode(mode: Raven.Client.ServerWide.Operations.Certificates.SecurityClearance) {
        this.securityClearance(mode);
    }


    toGenerateCertificateDto() {
        return {
            Name: this.name(),
            Password: this.certificatePassphrase(),
            Permissions: this.serializePermissions(),
            SecurityClearance: this.securityClearance()
            //TODO: expiration
        }
    }
    
    toUploadCertificateDto() {
        return {
            Name: this.name(),
            Certificate: this.certificateAsBase64(),
            Password: this.certificatePassphrase(),
            Permissions: this.serializePermissions(),
            SecurityClearance: this.securityClearance()
            //TODO: expiration
        }
    }

    toUpdatePermissionsDto() {
        return {
            Thumbprint: this.thumbprint(),
            SecurityClearance: this.securityClearance(),
            Permissions: this.serializePermissions()
        }
    }
    
    private serializePermissions() : dictionary<Raven.Client.ServerWide.Operations.Certificates.DatabaseAccess> {
        if (this.securityClearance() === "ClusterAdmin" || this.securityClearance() === "Operator") {
            return null;
        } 
        
        const result = {} as dictionary<Raven.Client.ServerWide.Operations.Certificates.DatabaseAccess>;
        this.permissions().forEach(permission => {
            result[permission.databaseName()] = permission.accessLevel();
        });
        
        return result;
    }
    
    static generate() {
        return new certificateModel("generate");
    }
    
    static upload() {
        return new certificateModel("upload");
    }
    
    static fromDto(dto: Raven.Client.ServerWide.Operations.Certificates.CertificateDefinition) {
        const model = new certificateModel("editExisting");
        model.name(dto.Name);
        model.securityClearance(dto.SecurityClearance);
        model.thumbprint(dto.Thumbprint);
        
        model.permissions(_.map(dto.Permissions, (access, databaseName) => {
            const permission = new certificatePermissionModel();
            permission.accessLevel(access);
            permission.databaseName(databaseName);
            return permission;
        }));
        return model;
    }
}

export = certificateModel;
