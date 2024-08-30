import appUrl = require("common/appUrl");
import router = require("plugins/router");
import saveReplicationSinkTaskCommand = require("commands/database/tasks/saveReplicationSinkTaskCommand");
import ongoingTaskReplicationSinkEditModel = require("models/database/tasks/ongoingTaskReplicationSinkEditModel");
import eventsCollector = require("common/eventsCollector");
import generalUtils = require("common/generalUtils");
import getConnectionStringsCommand = require("commands/database/settings/getConnectionStringsCommand");
import connectionStringRavenEtlModel = require("models/database/settings/connectionStringRavenEtlModel");
import jsonUtil = require("common/jsonUtil");
import getOngoingTaskInfoCommand = require("commands/database/tasks/getOngoingTaskInfoCommand");
import messagePublisher = require("common/messagePublisher");
import discoveryUrl = require("models/database/settings/discoveryUrl");
import replicationCertificateModel = require("models/database/tasks/replicationCertificateModel");
import forge = require("node-forge");
import fileImporter = require("common/fileImporter");
import popoverUtils = require("common/popoverUtils");
import prefixPathModel = require("models/database/tasks/prefixPathModel");
import endpoints = require("endpoints");
import getCertificatesCommand = require("commands/auth/getCertificatesCommand");
import accessManager = require("common/shell/accessManager");
import shardViewModelBase from "viewmodels/shardViewModelBase";
import database from "models/resources/database";
import licenseModel from "models/auth/licenseModel";
import { EditReplicationSinkInfoHub } from "viewmodels/database/tasks/EditReplicationSinkInfoHub";
import { sortBy } from "common/typeUtils";

class editReplicationSinkTask extends shardViewModelBase {

    view = require("views/database/tasks/editReplicationSinkTask.html");
    connectionStringView = require("views/database/settings/connectionStringRaven.html");
    taskResponsibleNodeSectionView = require("views/partial/taskResponsibleNodeSection.html");
    pinResponsibleNodeTextScriptView = require("views/partial/pinResponsibleNodeTextScript.html");

    editedSinkTask = ko.observable<ongoingTaskReplicationSinkEditModel>();
    isAddingNewTask = ko.observable<boolean>(true);
    private taskId: number = null;
    
    possibleMentors = ko.observableArray<string>([]);
    
    ravenEtlConnectionStringsDetails = ko.observableArray<Raven.Client.Documents.Operations.ETL.RavenConnectionString>([]);
    connectionStringsUrl = appUrl.forCurrentDatabase().connectionStrings();
    testConnectionResult = ko.observable<Raven.Server.Web.System.NodeConnectionTestResult>();
    
    spinners = { 
        test: ko.observable<boolean>(false),
        save: ko.observable<boolean>(false) 
    };

    fullErrorDetailsVisible = ko.observable<boolean>(false);
    shortErrorText: KnockoutObservable<string>;

    createNewConnectionString = ko.observable<boolean>(false);
    newConnectionString = ko.observable<connectionStringRavenEtlModel>();

    canDefineCertificates = accessManager.default.secureServer();
    serverCertificateModel = ko.observable<replicationCertificateModel>();
    exportCertificateUrl = endpoints.global.adminCertificates.adminCertificatesExport;
    private readonly serverCertificateName = "Server Certificate";
    
    hasPullReplicationAsSink = licenseModel.getStatusValue("HasPullReplicationAsSink");
    infoHubView: ReactInKnockout<typeof EditReplicationSinkInfoHub>

    constructor(db: database) {
        super(db);
        this.bindToCurrentInstance("useConnectionString", "onTestConnectionRaven", "onConfigurationFileSelected",
                                   "certFileSelected", "removeCertificate", "downloadServerCertificate", "setState");
        this.infoHubView = ko.pureComputed(() => ({
            component: EditReplicationSinkInfoHub
        }))
    }

    canActivate(args: any) {
        return $.when<any>(super.canActivate(args))
            .then(() => {
                const deferred = $.Deferred<canActivateResultDto>();

                if (this.canDefineCertificates) {
                    new getCertificatesCommand(false, false)
                        .execute()
                        .done(certificatesInfo => {
                            const serverCertificate = certificatesInfo.Certificates.find(cert => cert.Name === this.serverCertificateName);
                            this.serverCertificateModel(new replicationCertificateModel(serverCertificate.Certificate));
                            deferred.resolve({can: true});
                        })
                        .fail(() => {
                            deferred.resolve({ redirect: appUrl.forOngoingTasks(this.db) });
                        });
                } else {
                    deferred.resolve({can: true});
                }
                
                return deferred;
            });
    }

    activate(args: any) { 
        super.activate(args);
        const deferred = $.Deferred<void>();

        this.loadPossibleMentors();
        
        if (args.taskId) {
            // 1. Editing an existing task
            this.isAddingNewTask(false);
            this.taskId = args.taskId;

            getOngoingTaskInfoCommand.forPullReplicationSink(this.db, this.taskId)
                .execute()
                .done((result: Raven.Client.Documents.Operations.OngoingTasks.OngoingTaskPullReplicationAsSink) => {
                    
                    this.editedSinkTask(new ongoingTaskReplicationSinkEditModel(result, this.serverCertificateModel()));
                    const sinkAccess = this.editedSinkTask().replicationAccess();
                    sinkAccess.certificateExtracted(true);
                    
                    if (this.canDefineCertificates && !result.CertificatePublicKey) {
                        sinkAccess.useServerCertificate(true);
                    }
                    
                    deferred.resolve();
                })
                .fail(() => {
                    deferred.reject();
                    router.navigate(appUrl.forOngoingTasks(this.db));
                });
        } else {
            // 2. Creating a new task
            this.isAddingNewTask(true);
            this.editedSinkTask(ongoingTaskReplicationSinkEditModel.empty(this.serverCertificateModel()));
            deferred.resolve();
        }

        return $.when<any>(this.getAllConnectionStrings(), deferred)
            .done(() => this.initObservables());
    }

    private loadPossibleMentors() {
        const members = this.db.nodes()
            .filter(x => x.type === "Member")
            .map(x => x.tag);

        this.possibleMentors(members);
    }

    private getAllConnectionStrings() {
        return new getConnectionStringsCommand(this.db)
            .execute()
            .done((result: Raven.Client.Documents.Operations.ConnectionStrings.GetConnectionStringsResult) => {
                const connectionStrings = (<any>Object).values(result.RavenConnectionStrings);
                this.ravenEtlConnectionStringsDetails(sortBy(connectionStrings, x => x.Name.toUpperCase()));
            });
    }

    compositionComplete() {
        super.compositionComplete();

        $('.edit-pull-replication-sink-task [data-toggle="tooltip"]').tooltip();
        this.initTooltips();
    }

    private initObservables() {
        
        this.shortErrorText = ko.pureComputed(() => {
            const result = this.testConnectionResult();
            if (!result || result.Success) {
                return "";
            }
            return generalUtils.trimMessage(result.Error);
        });
        
        const model = this.editedSinkTask();
        
        this.dirtyFlag = new ko.DirtyFlag([
                model.taskName,
                model.taskState,
                model.manualChooseMentor,
                model.pinMentorNode,
                model.mentorNode,
                model.connectionStringName,
                this.createNewConnectionString,
                model.hubName,
                model.allowReplicationFromHubToSink,
                model.allowReplicationFromSinkToHub,
                model.replicationAccess().certificate,
                model.replicationAccess().serverCertificateSelected(),
                model.replicationAccess().replicationAccessName,
                model.replicationAccess().samePrefixesForBothDirections,
                model.replicationAccess().hubToSinkPrefixes,
                model.replicationAccess().sinkToHubPrefixes
            ], false, jsonUtil.newLineNormalizingHashFunction);

        this.newConnectionString(connectionStringRavenEtlModel.empty());
        this.newConnectionString().setNameUniquenessValidator(name => !this.ravenEtlConnectionStringsDetails().find(x => x.Name.toLocaleLowerCase() === name.toLocaleLowerCase()));

        const connectionStringName = this.editedSinkTask().connectionStringName();
        const connectionStringIsMissing = connectionStringName && !this.ravenEtlConnectionStringsDetails()
            .find(x => x.Name.toLocaleLowerCase() === connectionStringName.toLocaleLowerCase());
        
        if (!this.ravenEtlConnectionStringsDetails().length || connectionStringIsMissing) {
            this.createNewConnectionString(true);
        }

        if (connectionStringIsMissing) {
            // looks like user imported data w/o connection strings, prefill form with desired name
            this.newConnectionString().connectionStringName(connectionStringName);
            this.editedSinkTask().connectionStringName(null);
        }
        
        // Discard test connection result when needed
        this.createNewConnectionString.subscribe(() => this.testConnectionResult(null));
        this.newConnectionString().topologyDiscoveryUrls.subscribe(() => this.testConnectionResult(null));
        this.newConnectionString().inputUrl().discoveryUrlName.subscribe(() => this.testConnectionResult(null));
      
        const readDebounced = _.debounce(() => this.editedSinkTask().replicationAccess().tryReadCertificate(), 1500);
        
        model.replicationAccess().selectedFilePassphrase.subscribe(() => readDebounced());
    }

    private initTooltips() {
        popoverUtils.longWithHover($("#hub-to-sink-info"),
            {
                content:
                    "<ul class='no-margin padding'>" +
                        "<li><small>These ID paths define <strong>what documents the Sink allows the Hub to send</strong>.</small></li>" +
                        "<li><small>The documents will be sent from the Hub only if these paths are also allowed on the Hub task definition.</small></li>" +
                        "<li><small>To send all documents under some path use <code>&lt;path&gt;/*</code> or <code>&lt;path&gt;-*</code></small></li>" +
                    "</ul>"
            });

        popoverUtils.longWithHover($("#sink-to-hub-info"),
            {
                content:
                    "<ul class='no-margin padding'>" +
                        "<li><small>These ID paths define <strong>what documents are allowed to be sent from this Sink to the Hub</strong>.</small></li>" +
                        "<li><small>The documents will be sent to the Hub only if these paths are also allowed on the Hub task definition.</small></li>" +
                        "<li><small>To send all documents under some path use <code>&lt;path&gt;/*</code> or <code>&lt;path&gt;-*</code></small></li>" +
                    "</ul>"
            });

        popoverUtils.longWithHover($("#sink-certificate-info"),
            {
                content:
                    "<ul class='no-margin padding'>" +
                        "<li><small>The certificate is used to authenticate the Sink when connecting to the Hub.</small></li>" +
                        "<li><small>The Sink keeps both the <strong>public & private keys</strong>.</small></li>" +
                        "<li><small>The Hub task must contain a matching public key.</small></li>" +
                    "</ul>"
            });
        
        popoverUtils.longWithHover($("#download-server-certificate"),
            {
                content:
                    "<ul class='no-margin padding'>" +
                    "<li><small>Download the cluster's server certificate(s) as a *.pfx file.</small></li>" +
                    "<li><small>Only public keys are downloaded.</small></li>" +
                    "</ul>"
            });
    }

    saveTask() {
        let hasAnyErrors = false;
        
        // Save discovery URL if user forgot to hit 'add url' button
        if (this.createNewConnectionString() &&
            this.newConnectionString().inputUrl().discoveryUrlName() &&
            this.isValid(this.newConnectionString().inputUrl().validationGroup)) {
                this.newConnectionString().addDiscoveryUrlWithBlink();
        }
        
        // Validate *new connection string* (if relevant..) 
        if (this.createNewConnectionString()) {
            if (!this.isValid(this.newConnectionString().validationGroup)) {
                hasAnyErrors = true;
            } else {
                // Use the new connection string
                this.editedSinkTask().connectionStringName(this.newConnectionString().connectionStringName());
            }
        }

        // Validate *general form*
        if (!this.isValid(this.editedSinkTask().validationGroup)) {
            hasAnyErrors = true;
        }
        
        // Validate *replication access*
        if (this.canDefineCertificates && !this.isValid(this.editedSinkTask().replicationAccess().validationGroup)) {
            hasAnyErrors = true;
        }
       
        if (hasAnyErrors) {
            return false;
        }

        this.spinners.save(true);

        // All is well, Save connection string (if relevant..) 
        const savingNewStringAction = $.Deferred<void>();
        if (this.createNewConnectionString()) {
            this.newConnectionString()
                .saveConnectionString(this.db)
                .done(() => {
                    savingNewStringAction.resolve();
                })
                .fail(() => {
                    this.spinners.save(false);
                });
        } else {
            savingNewStringAction.resolve();
        }

        // if samePrefixes then use h2s prefixes for both
        const editedItem = this.editedSinkTask().replicationAccess();
        if (editedItem.samePrefixesForBothDirections()) {
            editedItem.sinkToHubPrefixes(editedItem.hubToSinkPrefixes());
        }
        
        // All is well, Save Replication task
        savingNewStringAction.done(() => {
            const dto = this.editedSinkTask().toDto(this.taskId);
            this.taskId = this.isAddingNewTask() ? 0 : this.taskId;

            eventsCollector.default.reportEvent("pull-replication-sink", "save");
            
            new saveReplicationSinkTaskCommand(this.db, dto)
                .execute()
                .done(() => {
                    this.dirtyFlag().reset();
                    this.goToOngoingTasksView();
                })
                .always(() => this.spinners.save(false));
        });
    }
   
    cancelOperation() {
        this.goToOngoingTasksView();
    }

    private goToOngoingTasksView() {
        router.navigate(appUrl.forOngoingTasks(this.db));
    }

    useConnectionString(connectionStringToUse: string) {
        this.editedSinkTask().connectionStringName(connectionStringToUse);
    }
    
    onTestConnectionRaven(urlToTest: discoveryUrl) {
        eventsCollector.default.reportEvent("pull-replication-sink", "test-connection");
        this.spinners.test(true);
        this.newConnectionString().selectedUrlToTest(urlToTest.discoveryUrlName());
        this.testConnectionResult(null);

        this.newConnectionString()
            .testConnection(urlToTest)
            .done(result => this.testConnectionResult(result))
            .always(() => {
                this.spinners.test(false);
                this.fullErrorDetailsVisible(false);
            });
    }

    onConfigurationFileSelected(fileInput: HTMLInputElement) {
        this.editedSinkTask().replicationAccess().useServerCertificate(false);
        fileImporter.readAsText(fileInput, data => this.importConfigurationFile(data));
    }
    
    private importConfigurationFile(contents: string) {
        try {
            let accessName: string;
            let certificate: replicationCertificateModel;
            let h2sPrefixes: Array<prefixPathModel>;
            let s2hPrefixes: Array<prefixPathModel>;
            let useSamePrefixes: boolean;
            
            const config = JSON.parse(contents) as pullReplicationExportFileFormat;
            
            if (!config.Database || !config.HubName || !config.TopologyUrls) {
                messagePublisher.reportError("Invalid configuration format");
                return;
            }

            const hubName = config.HubName;
            const h2sMode = config.AllowHubToSinkMode;
            const s2hMode = config.AllowSinkToHubMode;
            
            if (this.canDefineCertificates) {
                if (!config.AccessName || !config.HubToSinkPrefixes) {
                    messagePublisher.reportError("Invalid configuration format");
                    return;
                }

                accessName = config.AccessName;
                h2sPrefixes = config.HubToSinkPrefixes.map(x => new prefixPathModel(x));
                
                useSamePrefixes = false;
                if (config.UseSamePrefixes) {
                    s2hPrefixes = h2sPrefixes;
                    useSamePrefixes = true;
                } else if (config.SinkToHubPrefixes) {
                    s2hPrefixes = config.SinkToHubPrefixes.map(x => new prefixPathModel(x));
                }

                if (config.Certificate) {
                    certificate = replicationCertificateModel.fromPkcs12(config.Certificate);
                }
            }
            
            // update model only here (if no exception was thrown), otherwise we end up showing partial data...

            this.createNewConnectionString(true);
            const connectionString = this.newConnectionString();
            connectionString.database(config.Database);
            connectionString.connectionStringName("Connection string to " + config.Database);
            connectionString.topologyDiscoveryUrls(config.TopologyUrls.map(x => new discoveryUrl(x)));
            
            const model = this.editedSinkTask();
            
            model.hubName(hubName);
            model.allowReplicationFromHubToSink(h2sMode);
            model.allowReplicationFromSinkToHub(s2hMode);

            if (this.canDefineCertificates) {
                const accessInfo = model.replicationAccess();

                accessInfo.replicationAccessName(accessName);
                accessInfo.hubToSinkPrefixes(h2sPrefixes);
                accessInfo.sinkToHubPrefixes(s2hPrefixes);
                accessInfo.samePrefixesForBothDirections(useSamePrefixes);

                if (certificate) {
                    accessInfo.certificate(certificate);
                    accessInfo.certificateExtracted(true);
                    accessInfo.selectedFileName(null);
                    accessInfo.selectedFilePassphrase(null);
                }
            }
            
        } catch ($e) {
            messagePublisher.reportError("Can't parse configuration", $e);
            this.editedSinkTask().replicationAccess().certificateExtracted(false);
        }
    }

    certFileSelected(fileInput: HTMLInputElement) {
        fileImporter.readAsBinaryString(fileInput, (data, fileName) => {
            const isFileSelected = fileName ? !!fileName.trim() : false;

            const shortFileName = isFileSelected ? fileName.split(/(\\|\/)/g).pop() : null;
            this.editedSinkTask().replicationAccess().selectedFileName(shortFileName);

            const certAsBase64 = forge.util.encode64(data);
            this.editedSinkTask().replicationAccess().onCertificateSelected(certAsBase64);
        });
    }

    removeCertificate() {
        const model = this.editedSinkTask();
        
        model.replicationAccess().certificate(null);
        model.replicationAccess().certificateExtracted(false);
        
        model.replicationAccess().selectedFileName(null);
        model.replicationAccess().selectedFilePassphrase(null);
    }

    downloadServerCertificate() {
        const targetFrame = $("form#certificates_download_form");
        targetFrame.attr("action", this.exportCertificateUrl);
        targetFrame.submit();
    }

    setState(state: Raven.Client.Documents.Operations.OngoingTasks.OngoingTaskState): void {
        this.editedSinkTask().taskState(state);
    }
}

export = editReplicationSinkTask;
