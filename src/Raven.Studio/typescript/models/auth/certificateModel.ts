/// <reference path="../../../typings/tsd.d.ts" />

import certificatePermissionModel = require("models/auth/certificatePermissionModel");
import moment = require("moment");
import { sortBy } from "common/typeUtils";

type TwoFactorAction = "leave" | "set" | "delete";

class certificateModel {

    static allTimeUnits: Array<valueAndLabelItem<timeUnit, string>> = [
        {
            label: "days",
            value: "day"
        }, {
            label: "months",
            value: "month"
        }
    ];
    
    validityPeriodUnits = ko.observable<timeUnit>("month");
    validityPeriodUnitsLabel: KnockoutComputed<string>;
    
    static securityClearanceTypes: valueAndLabelItem<Raven.Client.ServerWide.Operations.Certificates.SecurityClearance, string>[] = [
        {
            label: "Cluster Admin",
            value: "ClusterAdmin"
        }, {
            label: "Operator", 
            value: "Operator"
        }, {
            label: "User",
            value: "ValidUser"
        }];
    
    static twoFactorEditModes: valueAndLabelItem<TwoFactorAction, string>[] = [
        {
            value: "set",
            label: "Update existing authentication key"
        },
        {
            value: "delete",
            label: "Delete existing authentication key"
        },
        {
            value: "leave",
            label: "Leave existing authentication key"
        }
    ]
    
    mode = ko.observable<certificateMode>();
    
    name = ko.observable<string>();
    securityClearance = ko.observable<Raven.Client.ServerWide.Operations.Certificates.SecurityClearance>("ValidUser");
    
    requireTwoFactor = ko.observable<boolean>(false);
    authenticationKey = ko.observable<string>("");
    hasTwoFactor = ko.observable<boolean>(false);
    twoFactorActionOnEdit = ko.observable<TwoFactorAction>("leave");
    
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
    twoFactorActionOnEditLabel: KnockoutComputed<string>;

    deleteExpired = ko.observable<boolean>(false);
    
    validationGroup: KnockoutValidationGroup = ko.validatedObservable({
        name: this.name,
        certificateAsBase64: this.certificateAsBase64,
        validityPeriod: this.validityPeriod
    });
    
    private constructor(mode: certificateMode) {
        this.mode(mode);

        _.bindAll(this, "setClearanceMode", "changeValidityPeriodUnits");
        
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
        
        this.twoFactorActionOnEditLabel = ko.pureComputed(() => 
            certificateModel.twoFactorEditModes.find(x => x.value === this.twoFactorActionOnEdit()).label
        );

        this.expirationDateFormatted = ko.pureComputed(() => {
            const validPeriod = this.validityPeriod();

            if (!validPeriod) {
                return null;
            }

            return moment.utc().add(validPeriod, this.validityPeriodUnitsLabel() as any).format();
        });
        
        this.validityPeriodUnitsLabel = ko.pureComputed(
            () => certificateModel.allTimeUnits.find(x => this.validityPeriodUnits() === x.value).label);
        
        this.twoFactorActionOnEdit.subscribe(action => {
            if (this.mode() === "editExisting") {
                switch (action) {
                    case "delete":
                        this.requireTwoFactor(false);
                        break;
                    case "set":
                        this.requireTwoFactor(true);
                        break;
                    case "leave":
                        this.requireTwoFactor(false);
                        break;
                }
            }
        })
    }
    
    static clearanceLabelFor(input: Raven.Client.ServerWide.Operations.Certificates.SecurityClearance) {
        if (input === "ClusterNode") {
            return "Cluster Node";
        }
        return certificateModel.securityClearanceTypes.find(x => x.value === input).label;
    }
    
    static resolveDatabasesAccess(certificateDefinition: Raven.Client.ServerWide.Operations.Certificates.CertificateDefinition): Array<databaseAccessInfo> {
        let dbAccessInfo;
        
        switch (certificateDefinition.SecurityClearance) {
            case "ClusterAdmin":
            case "Operator":
            case "ClusterNode":
                dbAccessInfo =  { All : "Admin"};
                break;
            default:
                dbAccessInfo = certificateDefinition.Permissions;
        }
        
        const dbAccessArray = Object.entries(dbAccessInfo).map(([dbName, accessLevel]) => 
            ({ accessLevel: `Database${accessLevel}` as databaseAccessLevel,  dbName: dbName }));
        
        return sortBy(dbAccessArray, x => x.dbName.toLowerCase());
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
            digit: true,
            min: 1
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
            NotAfter: this.expirationDateFormatted(),
            TwoFactorAuthenticationKey: this.requireTwoFactor() ? this.authenticationKey() : null,
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
            NotAfter: this.expirationDateFormatted(),
            TwoFactorAuthenticationKey: this.requireTwoFactor() ? this.authenticationKey() : null,
        }
    }

    toUpdatePermissionsDto() {
        return {
            Name: this.name(),
            Thumbprint: this.thumbprint(),
            SecurityClearance: this.securityClearance(),
            Permissions: this.serializePermissions(),
            TwoFactorAuthenticationKey: this.requireTwoFactor() ? this.authenticationKey() : null,
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

    static regenerate(itemToRegenerate: unifiedCertificateDefinition) {
        const newItem = new certificateModel("regenerate");
        
        newItem.name(itemToRegenerate.Name);
        newItem.thumbprint(itemToRegenerate.Thumbprint);
        newItem.securityClearance(itemToRegenerate.SecurityClearance);

        for (const dbItem in itemToRegenerate.Permissions) {
            const permission = new certificatePermissionModel();
            permission.databaseName(dbItem);
            permission.accessLevel(itemToRegenerate.Permissions[dbItem]);
            newItem.permissions.push(permission);
        }
        
        return newItem;
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
        model.hasTwoFactor(dto.HasTwoFactor);
        
        model.permissions(dto.Permissions ? Object.entries(dto.Permissions).map(([databaseName, access]) => {
            const permission = new certificatePermissionModel();
            permission.accessLevel(access);
            permission.databaseName(databaseName);
            return permission;
        }): []);
        return model;
    }

    changeValidityPeriodUnits(unit: timeUnit) {
        this.validityPeriodUnits(unit);
    }
}

export = certificateModel;
