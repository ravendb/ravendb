/// <reference path="../../../typings/tsd.d.ts" />

import certificatePermissionModel = require("models/auth/certificatePermissionModel");

class certificateModel {

    static securityClearanceTypes: valueAndLabelItem<Raven.Client.ServerWide.Operations.Certificates.SecurityClearance, string>[] = [
        {
            label: "Cluster Administrator",
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

    validityPeriod = ko.observable<number>();
    expirationDateFormatted: KnockoutComputed<string>;
    
    thumbprint = ko.observable<string>(); // primary cert thumbprint
    thumbprints = ko.observableArray<string>(); // all thumbprints
    
    replaceImmediately = ko.observable<boolean>(false);

    permissions = ko.observableArray<certificatePermissionModel>();

    securityClearanceLabel: KnockoutComputed<string>;
    canEditClearance: KnockoutComputed<boolean>;
    
    validationGroup: KnockoutValidationGroup = ko.validatedObservable({
        name: this.name,
        certificateAsBase64: this.certificateAsBase64,
        validityPeriod: this.validityPeriod
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
        });
        
        this.canEditClearance = ko.pureComputed(() => this.securityClearance() !== "ClusterNode");

        this.expirationDateFormatted = ko.pureComputed(() => {
            const validMonths = this.validityPeriod();

            if (!validMonths) {
                return null;
            }

            return moment.utc().add(validMonths, "months").format();
        });
    }
    
    static clearanceLabelFor(input: Raven.Client.ServerWide.Operations.Certificates.SecurityClearance) {
        if (input === "ClusterNode") {
            return "Cluster Node";
        }
        return certificateModel.securityClearanceTypes.find(x => x.value === input).label;
    }
    
    static resolveDatabasesAccess(certificateDefinition: Raven.Client.ServerWide.Operations.Certificates.CertificateDefinition): Array<string> {
        switch (certificateDefinition.SecurityClearance) {
            case "ClusterAdmin":
            case "Operator":
            case "ClusterNode":
                return ["All"];
            default:
                const access = Object.keys(certificateDefinition.Permissions);
                if (access.length) {
                    return _.sortBy(access, x => x.toLowerCase());
                }
                return [];
        }
    }

    private initValidation() {
        this.name.extend({
            required: {
                onlyIf: () => this.mode() !== "replace"
            }
        });
        
        this.certificateAsBase64.extend({
            required: {
                onlyIf: () => this.mode() === "upload" || this.mode() === 'replace'
            } 
        });

        this.validityPeriod.extend({
            digit: true
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
            SecurityClearance: this.securityClearance(),
            NotAfter: this.expirationDateFormatted()
        }
    }
    
    toReplaceCertificateDto() {
        return {
            Certificate: this.certificateAsBase64(),
            Password: this.certificatePassphrase(),
        }
    }
    
    toUploadCertificateDto() {
        return {
            Name: this.name(),
            Certificate: this.certificateAsBase64(),
            Password: this.certificatePassphrase(),
            Permissions: this.serializePermissions(),
            SecurityClearance: this.securityClearance(),
            NotAfter: this.expirationDateFormatted()
        }
    }

    toUpdatePermissionsDto() {
        return {
            Name: this.name(),
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
    
    static replace() {
        return new certificateModel("replace");
    }
    
    static fromDto(dto: unifiedCertificateDefinition) {
        const model = new certificateModel("editExisting");
        model.name(dto.Name);
        model.securityClearance(dto.SecurityClearance);
        model.thumbprint(dto.Thumbprint);
        model.thumbprints(dto.Thumbprints);
        
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
