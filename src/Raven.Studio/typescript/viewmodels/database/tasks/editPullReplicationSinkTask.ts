import appUrl = require("common/appUrl");
import viewModelBase = require("viewmodels/viewModelBase");
import router = require("plugins/router");
import savePullReplicationSinkTaskCommand = require("commands/database/tasks/savePullReplicationSinkTaskCommand");
import ongoingTaskPullReplicationSinkEditModel = require("models/database/tasks/ongoingTaskPullReplicationSinkEditModel");
import eventsCollector = require("common/eventsCollector");
import generalUtils = require("common/generalUtils");
import getConnectionStringsCommand = require("commands/database/settings/getConnectionStringsCommand");
import getPossibleMentorsCommand = require("commands/database/tasks/getPossibleMentorsCommand");
import connectionStringRavenEtlModel = require("models/database/settings/connectionStringRavenEtlModel");
import jsonUtil = require("common/jsonUtil");
import getOngoingTaskInfoCommand = require("commands/database/tasks/getOngoingTaskInfoCommand");
import messagePublisher = require("common/messagePublisher");
import discoveryUrl = require("models/database/settings/discoveryUrl");
import pullReplicationCertificate = require("models/database/tasks/pullReplicationCertificate");
import forge = require("forge/forge");

class editPullReplicationSinkTask extends viewModelBase {

    editedReplication = ko.observable<ongoingTaskPullReplicationSinkEditModel>();
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

    canDefineCertificates = location.protocol === "https:";
    
    importedFileName = ko.observable<string>();
    
    validationGroup = ko.validatedObservable({
        importedFileName: this.importedFileName
    });

    constructor() {
        super();
        this.bindToCurrentInstance("useConnectionString", "onTestConnectionRaven", "onConfigurationFileSelected", "deleteCertificate", "certFileSelected");
    }

    activate(args: any) { 
        super.activate(args);
        const deferred = $.Deferred<void>();

        if (args.taskId) {
            // 1. Editing an existing task
            this.isAddingNewTask(false);
            this.taskId = args.taskId;

            getOngoingTaskInfoCommand.forPullReplicationSink(this.activeDatabase(), this.taskId)
                .execute()
                .done((result: Raven.Client.Documents.Operations.OngoingTasks.OngoingTaskPullReplicationAsSink) => { 
                    this.editedReplication(new ongoingTaskPullReplicationSinkEditModel(result));
                    deferred.resolve();
                })
                .fail(() => {
                    deferred.reject();
                    
                    router.navigate(appUrl.forOngoingTasks(this.activeDatabase()));
                });
        } else {
            // 2. Creating a new task
            this.isAddingNewTask(true);
            this.editedReplication(ongoingTaskPullReplicationSinkEditModel.empty());
            deferred.resolve();
        }

        deferred.done(() => this.initObservables());
        
        return $.when<any>(this.getAllConnectionStrings(), this.loadPossibleMentors(), deferred);
    }
    
    private loadPossibleMentors() {
        return new getPossibleMentorsCommand(this.activeDatabase().name)
            .execute()
            .done(mentors => this.possibleMentors(mentors));
    }

    private getAllConnectionStrings() {
        return new getConnectionStringsCommand(this.activeDatabase())
            .execute()
            .done((result: Raven.Client.Documents.Operations.ConnectionStrings.GetConnectionStringsResult) => {
                const connectionStrings = (<any>Object).values(result.RavenConnectionStrings);
                this.ravenEtlConnectionStringsDetails(_.sortBy(connectionStrings, x => x.Name.toUpperCase()));                
            });
    }

    private initObservables() {
        if (this.canDefineCertificates) {
            this.importedFileName.extend({
                validation: [
                    {
                        validator: () => !!this.editedReplication().certificate(),
                        message: "Certificate is required"
                    }
                ]
            });
        }
        
        this.shortErrorText = ko.pureComputed(() => {
            const result = this.testConnectionResult();
            if (!result || result.Success) {
                return "";
            }
            return generalUtils.trimMessage(result.Error);
        });
        
        const model = this.editedReplication();
        
        this.dirtyFlag = new ko.DirtyFlag([
                model.taskName,
                model.manualChooseMentor,
                model.preferredMentor,
                model.connectionStringName,
                model.hubDefinitionName,
                model.certificate,
                this.createNewConnectionString
            ], false, jsonUtil.newLineNormalizingHashFunction);

        this.newConnectionString(connectionStringRavenEtlModel.empty());

        // Open the 'Create new conn. str.' area if no connection strings are yet defined 
        this.ravenEtlConnectionStringsDetails.subscribe((value) => { this.createNewConnectionString(!value.length) }); 
        
        // Discard test connection result when needed
        this.createNewConnectionString.subscribe(() => this.testConnectionResult(null));
        this.newConnectionString().inputUrl().discoveryUrlName.subscribe(() => this.testConnectionResult(null));
        
        const readDebounced = _.debounce(() => this.tryReadCertificate(), 1500);
        
        model.certificatePassphrase.subscribe(() => readDebounced());
    }

    compositionComplete() {
        super.compositionComplete();
        document.getElementById('taskName').focus();
        
        $('.edit-pull-replication-sink-task [data-toggle="tooltip"]').tooltip();
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
                this.editedReplication().connectionStringName(this.newConnectionString().connectionStringName());
            }
        }

        // Validate *general form*
        if (!this.isValid(this.editedReplication().validationGroup)) {
            hasAnyErrors = true;
        }
        
        // Validate *local* form
        if (!this.isValid(this.validationGroup)) {
            hasAnyErrors = true;
        }
       
        if (hasAnyErrors) {
            return false;
        }

        this.spinners.save(true);

        // All is well, Save connection string (if relevant..) 
        let savingNewStringAction = $.Deferred<void>();
        if (this.createNewConnectionString()) {
            this.newConnectionString()
                .saveConnectionString(this.activeDatabase())
                .done(() => {
                    savingNewStringAction.resolve();
                })
                .fail(() => {
                    this.spinners.save(false);
                });
        } else {
            savingNewStringAction.resolve();
        }
        
        // All is well, Save Replication task
        savingNewStringAction.done(() => {
            const dto = this.editedReplication().toDto(this.taskId);
            this.taskId = this.isAddingNewTask() ? 0 : this.taskId;

            eventsCollector.default.reportEvent("pull-replication-sink", "save");
            
            new savePullReplicationSinkTaskCommand(this.activeDatabase(), dto)
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
        router.navigate(appUrl.forOngoingTasks(this.activeDatabase()));
    }

    useConnectionString(connectionStringToUse: string) {
        this.editedReplication().connectionStringName(connectionStringToUse);
    }
    
    onTestConnectionRaven(urlToTest: string) {
        eventsCollector.default.reportEvent("pull-replication-sink", "test-connection");
        this.spinners.test(true);
        this.newConnectionString().selectedUrlToTest(urlToTest);
        this.testConnectionResult(null);

        this.newConnectionString()
            .testConnection(urlToTest)
            .done(result => this.testConnectionResult(result))
            .always(() => {
                this.spinners.test(false);
                this.newConnectionString().selectedUrlToTest(null);
            });
    }

    onConfigurationFileSelected() {
        const fileInput = <HTMLInputElement>document.querySelector("#configurationFilePicker");
        const self = this;
        if (fileInput.files.length === 0) {
            return;
        }

        const file = fileInput.files[0];
        const reader = new FileReader();
        reader.onload = function() {
// ReSharper disable once SuspiciousThisUsage
            self.importUsingFile(this.result as string);
        };
        reader.onerror = function(error: any) {
            alert(error);
        };
        reader.readAsText(file);

        const $input = $("#configurationFilePicker");
        $input.val(null);
    }
    
    private importUsingFile(contents: string) {
        
        try {
            const config = JSON.parse(contents) as pullReplicationExportFileFormat;
            
            if (!config.Database || !config.HubDefinitionName || !config.TopologyUrls) {
                messagePublisher.reportError("Invalid configuration format");
                return;
            }
            
            const model = this.editedReplication();
            model.hubDefinitionName(config.HubDefinitionName);
            
            if (config.Certificate) {
                model.certificate(pullReplicationCertificate.fromPkcs12(config.Certificate));
            }
            
            this.createNewConnectionString(true);
            const connectionString = this.newConnectionString();
            connectionString.database(config.Database);
            connectionString.connectionStringName("Pull replication from " + config.Database);
            connectionString.topologyDiscoveryUrls(config.TopologyUrls.map(x => new discoveryUrl(x)));
            
        } catch (e) {
            messagePublisher.reportError("Can't parse configuration");
        }
    }
    
    deleteCertificate() {
        this.editedReplication().certificate(null);
    }

    certFileSelected(fileInput: HTMLInputElement) {
        const self = this;
        if (fileInput.files.length === 0) {
            return;
        }

        const file = fileInput.files[0];

        const fileName = fileInput.value;
        const isFileSelected = fileName ? !!fileName.trim() : false;
        this.importedFileName(isFileSelected ? fileName.split(/(\\|\/)/g).pop() : null);
        
        const reader = new FileReader();
        reader.onload = function() {
// ReSharper disable once SuspiciousThisUsage
            const asBase64 = forge.util.encode64(this.result);
            self.onCertificateLoaded(asBase64);
        };
        reader.onerror = function(error: any) {
            alert(error);
        };
        reader.readAsBinaryString(file);
    }
    
    onCertificateLoaded(certAsBase64: string) {
        this.editedReplication().certificateAsBase64(certAsBase64);
     
        this.tryReadCertificate();
    }
    
    tryReadCertificate() {
        if (this.editedReplication().tryReadCertificate()) {
            this.importedFileName(undefined);
        }
    }
    
   
}

export = editPullReplicationSinkTask;
