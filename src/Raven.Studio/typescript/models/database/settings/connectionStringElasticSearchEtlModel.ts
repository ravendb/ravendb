/// <reference path="../../../../typings/tsd.d.ts"/>
import database = require("models/resources/database");
import connectionStringModel = require("models/database/settings/connectionStringModel");
import saveConnectionStringCommand_OLD = require("commands/database/settings/saveConnectionStringCommand_OLD");
import jsonUtil = require("common/jsonUtil");
import discoveryUrl = require("models/database/settings/discoveryUrl");
import fileImporter = require("common/fileImporter");
import replicationCertificateModel = require("models/database/tasks/replicationCertificateModel");
import forge = require("node-forge");
import certificateUtils = require("common/certificateUtils");
import messagePublisher = require("common/messagePublisher");
import testElasticSearchNodeConnectionCommand = require("commands/database/cluster/testElasticSearchNodeConnectionCommand");

type authenticationMethod = "none" | "basic" | "apiKey" | "encodedApiKey" | "certificate";

class authenticationInfo {

    static authProviders: Array<valueAndLabelItem<authenticationMethod, string>> = [
        { value: "none", label: "No authentication" },
        { value: "basic", label: "Basic" },
        { value: "apiKey",label: "API Key" },
        { value: "encodedApiKey",label: "Encoded API Key" },
        { value: "certificate", label: "Certificate" }
    ];
    
    authMethodUsed = ko.observable<authenticationMethod>();
    
    username = ko.observable<string>();
    password = ko.observable<string>();

    apiKeyId = ko.observable<string>();
    apiKey = ko.observable<string>();
    
    encodedApiKey = ko.observable<string>();

    certificates = ko.observableArray<replicationCertificateModel>([]);
    
    dirtyFlag: () => DirtyFlag;
    validationGroup: KnockoutValidationGroup;

    constructor(dto: Raven.Client.Documents.Operations.ETL.ElasticSearch.Authentication) {
        if (dto.Basic) {
            this.username(dto.Basic.Username);
            this.password(dto.Basic.Password);
        }

        if (dto.ApiKey?.EncodedApiKey) {
            this.encodedApiKey(dto.ApiKey.EncodedApiKey);
        } 
        
        if (dto.ApiKey) {
            this.apiKeyId(dto.ApiKey.ApiKeyId);
            this.apiKey(dto.ApiKey.ApiKey);
        }
        
        if (dto.Certificate) {
            dto.Certificate.CertificatesBase64.forEach(x => {
                const certificateModel = new replicationCertificateModel(x);
                this.certificates.push(certificateModel);
            });
        }
        
        this.initObservables();
        this.initValidation();
    }
    
    private initObservables(): void {
        this.authMethodUsed(this.findMethodUsed());
        
        this.dirtyFlag = new ko.DirtyFlag([
            this.username,
            this.password,
            this.apiKeyId,
            this.apiKey,
            this.encodedApiKey,
            this.certificates,
            this.authMethodUsed
        ], false, jsonUtil.newLineNormalizingHashFunction);
    }
    
    private findMethodUsed(): authenticationMethod {
        if (this.encodedApiKey()) {
            return "encodedApiKey";
        }
        
        if (this.username() && this.password()) {
            return "basic";
        }
        
        if (this.apiKeyId() && this.apiKey()) {
            return "apiKey";
        }
        
        if (this.certificates() && this.certificates().length) {
            return "certificate";
        }
        
        return "none";
    }
    
    private initValidation(): void {
        this.username.extend({
            required: {
                onlyIf: () => this.authMethodUsed() === "basic"
            }
        });
        
        this.password.extend({
            required: {
                onlyIf: () => this.authMethodUsed() === "basic"
            }
        });

        this.apiKeyId.extend({
            required: {
                onlyIf: () => this.authMethodUsed() === "apiKey"
            }
        });

        this.apiKey.extend({
            required: {
                onlyIf: () => this.authMethodUsed() === "apiKey"
            }
        });
        
        this.encodedApiKey.extend({
            required: {
                onlyIf: () => this.authMethodUsed() === "encodedApiKey"
            }
        })

        this.certificates.extend({
            validation: [
                {
                    validator: () => this.authMethodUsed() !== "certificate" || this.certificates().length > 0,
                    message: `Certificate not uploaded`
                }
            ]
        });
        
        this.validationGroup = ko.validatedObservable({
            username: this.username,
            password: this.password,
            apiKeyId: this.apiKeyId,
            apiKey: this.apiKey,
            encodedApiKey: this.encodedApiKey,
            certificates: this.certificates
        });
    }

    toDto(): Raven.Client.Documents.Operations.ETL.ElasticSearch.Authentication {
        const methodUsed = this.authMethodUsed();

        return {
            Basic: methodUsed === "basic" ? { Username: this.username(), Password: this.password() } : null,
            ApiKey: methodUsed === "apiKey" || methodUsed === "encodedApiKey" ? { ApiKey: this.apiKey(), ApiKeyId: this.apiKeyId(), EncodedApiKey: this.encodedApiKey() } : null,
            Certificate: methodUsed === "certificate" ? { CertificatesBase64: this.certificates().map(x => x.publicKey()) } : null
        }
    }
    
    static empty(): authenticationInfo {
        return new authenticationInfo({
            Basic: {
                Username: null,
                Password: null,
            },
            ApiKey: {
                ApiKeyId: null,
                ApiKey: null,
                EncodedApiKey: null
            },
            Certificate: {
                CertificatesBase64: []
            }
        });
    }

    labelFor(input: string) {
        const provider = authenticationInfo.authProviders.find(x => x.value === input);
        return provider ? provider.label : null;
    }

    uploadElasticCertificate(fileInput: HTMLInputElement): void {
        fileImporter.readAsBinaryString(fileInput, data => this.onCertificateUploaded(data));
    }

    private onCertificateUploaded(data: string): void {
        
        try {
            // First detect the data format, pfx (binary) or crt/cer (text)
            // The line bellow will throw if data is not pfx
            forge.asn1.fromDer(data);

            // *** Handle pfx ***
            try {
                const certAsBase64 = forge.util.encode64(data);
                const certificatesArray = certificateUtils.extractCertificatesFromPkcs12(certAsBase64, undefined);
                
                certificatesArray.forEach(publicKey => {
                    const certificateModel = new replicationCertificateModel(publicKey, certAsBase64);
                    this.certificates.push(certificateModel);
                });
            } catch ($ex1) {
                messagePublisher.reportError("Unable to upload certificate", $ex1);
            }
            
        } catch {
            
            // *** Handle crt/cer *** 
            try {
                const certificateModel = new replicationCertificateModel(data);
                this.certificates.push(certificateModel);
            } catch ($ex2) {
                messagePublisher.reportError("Unable to upload certificate", $ex2);
            }
        }
    }

    removeCertificate(certModel: replicationCertificateModel) {
        const filtered = this.certificates().filter(x => x.thumbprint() !== certModel.thumbprint());
        this.certificates(filtered);
    }
}

class connectionStringElasticSearchEtlModel extends connectionStringModel {
    
    nodesUrls = ko.observableArray<discoveryUrl>([]);
    inputUrl = ko.observable<discoveryUrl>(new discoveryUrl(""));
    selectedUrlToTest = ko.observable<string>();
    
    authentication = ko.observable<authenticationInfo>();

    validationGroup: KnockoutValidationGroup;

    dirtyFlag: () => DirtyFlag;

    constructor(dto: Raven.Client.Documents.Operations.ETL.ElasticSearch.ElasticSearchConnectionString, isNew: boolean, tasks: { taskName: string; taskId: number }[]) {
        super(isNew, tasks);

        this.update(dto);
        this.initValidation();
        this.initObservables();
    }

    update(dto: Raven.Client.Documents.Operations.ETL.ElasticSearch.ElasticSearchConnectionString): void {
        super.update(dto);

        this.connectionStringName(dto.Name);
        this.nodesUrls(dto.Nodes.map((x) => new discoveryUrl(x)));

        this.authentication(new authenticationInfo(dto.Authentication));
    }
    
    initValidation(): void {
        super.initValidation();

        this.nodesUrls.extend({
            validation: [
                {
                    validator: () => this.nodesUrls().length > 0,
                    message: "At least one Elasticsearch node URL is required. Enter URL and click Add."
                }
            ]
        });

        this.validationGroup = ko.validatedObservable({
            connectionStringName: this.connectionStringName,
            nodesUrls: this.nodesUrls
        });
    }
    
    private initObservables(): void {
        const urlsCount = ko.pureComputed(() => this.nodesUrls().length);
        const urlsAreDirty = ko.pureComputed(() => {
            let anyDirty = false;

            this.nodesUrls().forEach(url => {
                if (url.dirtyFlag().isDirty()) {
                    anyDirty = true;
                }
            });

            return anyDirty;
        });

        this.dirtyFlag = new ko.DirtyFlag([
            this.connectionStringName,
            urlsCount,
            urlsAreDirty,
            this.authentication().dirtyFlag().isDirty,
        ], false, jsonUtil.newLineNormalizingHashFunction);
    }

    static empty(): connectionStringElasticSearchEtlModel {
        return new connectionStringElasticSearchEtlModel({
            Type: "ElasticSearch",
            Name: "",
            Nodes: [],
            Authentication: authenticationInfo.empty().toDto(),
        } as Raven.Client.Documents.Operations.ETL.ElasticSearch.ElasticSearchConnectionString, true, []);
    }

    toDto(): Raven.Client.Documents.Operations.ETL.ElasticSearch.ElasticSearchConnectionString {
        return {
            Type: "ElasticSearch",
            Name: this.connectionStringName(),
            Nodes: this.nodesUrls().map((x) => x.discoveryUrlName()),
            Authentication: this.authentication().toDto(),
            EnableCompatibilityMode: undefined // this field is no longer used
        };
    }

    removeDiscoveryUrl(url: discoveryUrl) {
        this.nodesUrls.remove(url);
    }

    addDiscoveryUrlWithBlink() {
        if (!this.nodesUrls().find(x => x.discoveryUrlName() === this.inputUrl().discoveryUrlName())) {
            const newUrl = new discoveryUrl(this.inputUrl().discoveryUrlName());
            newUrl.dirtyFlag().forceDirty();
            this.nodesUrls.unshift(newUrl);
            this.inputUrl().discoveryUrlName("");

            $(".collection-list li").first().addClass("blink-style");
        }
    }

    testConnection(db: database, urlToTest: discoveryUrl): JQueryPromise<Raven.Server.Web.System.NodeConnectionTestResult> {
        return new testElasticSearchNodeConnectionCommand(db, urlToTest.discoveryUrlName(), this.authentication().toDto())
            .execute()
            .done((result) => {
                if (result.Error) {
                    urlToTest.hasTestError(true);
                }
            });
    }

    saveConnectionString(db: database): JQueryPromise<void> {
        return new saveConnectionStringCommand_OLD(db, this)
            .execute();
    }
}

export = connectionStringElasticSearchEtlModel;
