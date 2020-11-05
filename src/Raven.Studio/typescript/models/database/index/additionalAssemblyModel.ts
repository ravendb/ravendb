/// <reference path="../../../../typings/tsd.d.ts"/>
import jsonUtil = require("common/jsonUtil");

type assemblySource = "Server Runtime" | "Path" | "NuGet";

class additionalAssemblyModel {
    
    assemblySource = ko.observable<assemblySource>();
    
    assemblyName = ko.observable<string>();
    assemblyPath = ko.observable<string>();
    
    packageName = ko.observable<string>();
    packageVersion = ko.observable<string>();
    packageSourceUrl = ko.observable<string>();
    useDefaultPackageSourceUrl = ko.observable<boolean>();
    
    usings = ko.observableArray<string>();
    namespaceText = ko.observable<string>();

    dirtyFlag: () => DirtyFlag;
    validationGroup: KnockoutObservable<any>;
    
    constructor(dto: Raven.Client.Documents.Indexes.AdditionalAssembly) {
        this.assemblyName(dto.AssemblyName);
        this.assemblyPath(dto.AssemblyPath);
        
        this.packageName(dto.PackageName);
        this.packageVersion(dto.PackageVersion);
        
        this.packageSourceUrl(dto.PackageSourceUrl);
        this.useDefaultPackageSourceUrl(!dto.PackageSourceUrl)
        
        this.usings(dto.Usings);

        this.assemblySource(this.computeAssemblySource());
       
        this.initObservables();
        this.initValidation();
    }

    private initObservables() {
        this.useDefaultPackageSourceUrl.subscribe((useDefault) => {
            if (useDefault) {
                this.packageSourceUrl(null);
            }
        });
        
        this.dirtyFlag = new ko.DirtyFlag([
            this.assemblySource,
            this.assemblyName,
            this.assemblyPath,
            this.packageName,
            this.packageVersion,
            this.packageSourceUrl,
            this.usings
        ], false, jsonUtil.newLineNormalizingHashFunction);
    }
    
    private initValidation() {
        this.assemblyName.extend({
            required: {
                onlyIf: () => this.assemblySource() === "Server Runtime"
            }
        });

        this.assemblyPath.extend({
            required: {
                onlyIf: () => this.assemblySource() === "Path"
            }
        });

        this.packageName.extend({
            required: {
                onlyIf: () => this.assemblySource() === "NuGet"
            }
        });

        this.packageVersion.extend({
            required: {
                onlyIf: () => this.assemblySource() === "NuGet"
            }
        });

        this.validationGroup = ko.validatedObservable({
            assemblyName: this.assemblyName,
            assemblyPath: this.assemblyPath,
            packageName: this.packageName,
            packageVersion: this.packageVersion
        });
    }
    
    private computeAssemblySource(): assemblySource {
        if (this.packageName()) {
            return "NuGet";
        }
        
        if (this.assemblyPath()) {
            return "Path";
        }
        
        return "Server Runtime";
    }

    setAssemblySourceType(sourceType: assemblySource) {
        this.assemblySource(sourceType);
    }

    addNamespaceToUsings(namespace: string) {
        if (!this.usings().find(x => x === namespace)) {
            this.usings.unshift(namespace);
            this.namespaceText(null);
            return true;
        }
        
        return false;
    }    

    removeNamespaceFromUsings(namespace: string) {
        const namespaceToRemove = this.usings().find(x => x === namespace);
        this.usings.remove(namespaceToRemove);
    }
    
    static empty(): additionalAssemblyModel {
        return new additionalAssemblyModel({
            AssemblyName: null,
            AssemblyPath: null,
            PackageName: null,
            PackageVersion: null,
            PackageSourceUrl: null,
            Usings: []
        }); 
    }
    
    toDto(): Raven.Client.Documents.Indexes.AdditionalAssembly {
        let result: Partial<Raven.Client.Documents.Indexes.AdditionalAssembly>;
        
        switch (this.assemblySource()) {
            case "Server Runtime":
                result = {
                    AssemblyName: this.assemblyName(),
                    Usings: this.usings()
                };
                break;
            case "Path":
                result = {
                    AssemblyPath: this.assemblyPath(),
                    Usings: this.usings()
                };
                break;
            case "NuGet":
                result = {
                    PackageName: this.packageName(),
                    PackageVersion: this.packageVersion(),
                    PackageSourceUrl: this.packageSourceUrl(),
                    Usings: this.usings()
                };
                break;
        }
        
        return result as Raven.Client.Documents.Indexes.AdditionalAssembly;
    }
}

export = additionalAssemblyModel;
