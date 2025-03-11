import app = require("durandal/app");
import appUrl = require("common/appUrl");
import router = require("plugins/router");
import database = require("models/resources/database");
import getOngoingTaskInfoCommand = require("commands/database/tasks/getOngoingTaskInfoCommand");
import getConnectionStringsCommand = require("commands/database/settings/getConnectionStringsCommand");
import saveEtlTaskCommand = require("commands/database/tasks/saveEtlTaskCommand");
import collectionsTracker = require("common/helpers/database/collectionsTracker");
import transformationScriptSyntax = require("viewmodels/database/tasks/transformationScriptSyntax");
import aceEditorBindingHandler = require("common/bindingHelpers/aceEditorBindingHandler");
import jsonUtil = require("common/jsonUtil");
import shardViewModelBase = require("viewmodels/shardViewModelBase");
import licenseModel = require("models/auth/licenseModel");
import EditEmbeddingsGenerationInfoHub = require("viewmodels/database/tasks/EditEmbeddingsGenerationInfoHub");
import typeUtils = require("common/typeUtils");
import ongoingTaskEmbeddingsGenerationEditModel = require("models/database/tasks/ongoingTaskEmbeddingsGenerationEditModel");
import EditConnectionStrings = require("components/pages/database/settings/connectionStrings/EditConnectionStrings");
import connectionStringsSlice = require("components/pages/database/settings/connectionStrings/store/connectionStringsSlice");
import storeCompat = require("components/storeCompat");
import getExpirationConfigurationCommand = require("commands/database/documents/getExpirationConfigurationCommand");
import saveExpirationConfigurationCommand = require("commands/database/documents/saveExpirationConfigurationCommand");
import DocumentExpiration = require("components/pages/database/settings/documentExpiration/DocumentExpiration");
import activeDatabaseTracker = require("common/shell/activeDatabaseTracker");

class editEmbeddingsGenerationTask extends shardViewModelBase {
    
    view = require("views/database/tasks/editEmbeddingsGenerationTask.html");
    taskResponsibleNodeSectionView = require("views/partial/taskResponsibleNodeSection.html");
    pinResponsibleNodeTextScriptView = require("views/partial/pinResponsibleNodeTextScript.html");

    editedEmbeddingsGeneration = ko.observable<ongoingTaskEmbeddingsGenerationEditModel>();
    isAddingNewEtlTask = ko.observable<boolean>(true);

    possibleMentors = ko.observableArray<string>([]);
    connectionStringsNames = ko.observableArray<string>([]);
    aiConnectionStrings = ko.observableArray<Raven.Client.Documents.Operations.AI.AiConnectionString>([]);

    spinners = {
        save: ko.observable<boolean>(false)
    };

    collections = collectionsTracker.default.collections;
    
    fullErrorDetailsVisible = ko.observable<boolean>(false);
    shortErrorText: KnockoutObservable<string>;
    
    collectionNames: KnockoutComputed<string[]>;

    showEditTransformationArea: KnockoutComputed<boolean>;
   
    hasAiIntegrations = licenseModel.getStatusValue("HasAiIntegrations");
    infoHubView: ReactInKnockout<typeof EditEmbeddingsGenerationInfoHub.EditEmbeddingsGenerationInfoHub>;

    isNewConnectionStringOpen = ko.observable<boolean>(false);
    newConnectionStringView: ReactInKnockout<typeof EditConnectionStrings.default>;

    sourceView = ko.observable<EditAiTaskSourceView>();

    isDocumentExpirationEnabled = ko.observable<boolean>(false);
    enableDocumentExpiration = ko.observable<boolean>(false);

    isAccordionOpen = ko.observable<boolean>(false);

    constructor(db: database) {
        super(db);
        this.bindToCurrentInstance(
            "useConnectionString",
            "syntaxHelp",
            "toggleIsNewConnectionStringOpen",
            "setState",
            "getIsDocumentExpirationEnabled",
            "toggleAccordion"
        );
        
        aceEditorBindingHandler.install();

        this.infoHubView = ko.pureComputed(() => ({
            component: EditEmbeddingsGenerationInfoHub.EditEmbeddingsGenerationInfoHub
        }));

        this.newConnectionStringView = ko.pureComputed(() => ({
            component: EditConnectionStrings.default,
            props: {
                initialConnection: {
                    type: "Ai"
                },
                afterSave: async (name: string) => {
                    await this.getAllConnectionStrings();
                    this.editedEmbeddingsGeneration().connectionStringName(name)
                    this.toggleIsNewConnectionStringOpen();
                },
                afterClose: () => {
                    this.toggleIsNewConnectionStringOpen();
                }
            }
        }))
    }

    toggleAccordion() {
        this.isAccordionOpen(!this.isAccordionOpen());
    }

    activate(args: { taskId?: number, sourceView: EditAiTaskSourceView }) {
        super.activate(args);
        const deferred = $.Deferred<void>();

        storeCompat.globalDispatch(connectionStringsSlice.connectionStringsActions.viewContextSet("ai"));
        this.sourceView(args.sourceView);
        
        this.loadPossibleMentors();

        if (args.taskId) {
            // 1. Editing an Existing task
            this.isAddingNewEtlTask(false);

            getOngoingTaskInfoCommand.forAiIntegration(this.db, args.taskId)
                .execute()
                .done((result) => {
                    this.editedEmbeddingsGeneration(new ongoingTaskEmbeddingsGenerationEditModel(result, this.aiConnectionStrings));

                    this.editedEmbeddingsGeneration().collectionInput.subscribe(() => this.editedEmbeddingsGeneration().setResetScriptIfEdit());;
                    this.editedEmbeddingsGeneration().embeddingsSource.subscribe(() => this.editedEmbeddingsGeneration().setResetScriptIfEdit());;
                    this.editedEmbeddingsGeneration().script.subscribe(() => this.editedEmbeddingsGeneration().setResetScriptIfEdit());;
                    this.editedEmbeddingsGeneration().embeddingPathConfigurations.subscribe(() => this.editedEmbeddingsGeneration().setResetScriptIfEdit());;
                    this.editedEmbeddingsGeneration().quantizationType.subscribe(() => this.editedEmbeddingsGeneration().setResetScriptIfEdit());;

                    deferred.resolve();
                })
                .fail(() => {
                    deferred.reject();
                    router.navigate(appUrl.forOngoingTasks(this.db));
                });
        } else {
            // 2. Creating a New task
            this.isAddingNewEtlTask(true);
            this.editedEmbeddingsGeneration(ongoingTaskEmbeddingsGenerationEditModel.empty(this.aiConnectionStrings));

            deferred.resolve();
        }
        
        return $.when<any>(this.getAllConnectionStrings(), this.getIsDocumentExpirationEnabled(), deferred)
            .done(() => {
                this.initObservables();
            });
    }

    private loadPossibleMentors() {
        const members = this.db.nodes()
            .filter(x => x.type === "Member")
            .map(x => x.tag);

        this.possibleMentors(members);
    }
    
    compositionComplete() {
        super.compositionComplete();

        $('.edit-ai-task [data-toggle="tooltip"]').tooltip();
    }

    toggleIsNewConnectionStringOpen() {
        this.isNewConnectionStringOpen(!this.isNewConnectionStringOpen())
    }

    private getAllConnectionStrings() {
        return new getConnectionStringsCommand(this.db)
            .execute()
            .done((result: Raven.Client.Documents.Operations.ConnectionStrings.GetConnectionStringsResult) => {
                this.aiConnectionStrings(Object.values(result.AiConnectionStrings));

                const connectionStringsNames = Object.keys(result.AiConnectionStrings);
                this.connectionStringsNames(typeUtils.sortBy(connectionStringsNames, x => x.toUpperCase()));
            });
    }

    private initObservables() {

        this.collectionNames = ko.pureComputed(() => {
            return collectionsTracker.default.getCollectionNames();
        });

        const connectionStringName = this.editedEmbeddingsGeneration().connectionStringName();
        const connectionStringIsMissing = connectionStringName && !this.connectionStringsNames()
            .find(x => x.toLocaleLowerCase() === connectionStringName.toLocaleLowerCase());

        if (connectionStringIsMissing) {
            // looks like user imported data w/o connection strings, prefill form with desired name
            this.editedEmbeddingsGeneration().connectionStringName(null);
        }

        this.initDirtyFlag();
    }
    
    private initDirtyFlag() {
        const innerDirtyFlag = ko.pureComputed(() => this.editedEmbeddingsGeneration().dirtyFlag().isDirty());

        this.dirtyFlag = new ko.DirtyFlag([
            innerDirtyFlag,
        ], false, jsonUtil.newLineNormalizingHashFunction);
    }
    
    useConnectionString(connectionStringToUse: string) {
        this.editedEmbeddingsGeneration().connectionStringName(connectionStringToUse);
    }

    async getIsDocumentExpirationEnabled() {
        const result = await new getExpirationConfigurationCommand(this.db).execute();

        if (!result) {
            this.enableDocumentExpiration(true);
            return this.isDocumentExpirationEnabled(false);
        }

        this.enableDocumentExpiration(result.Disabled);
        return this.isDocumentExpirationEnabled(!result.Disabled);
    }

    async saveEtl() {
        let hasAnyErrors = false;
        this.spinners.save(true);

        if (!this.isValid(this.editedEmbeddingsGeneration().validationGroup)) {
            hasAnyErrors = true;
        }
        
        if (hasAnyErrors) {
            this.spinners.save(false);
            return false;
        }
        
        const scriptsToReset = this.editedEmbeddingsGeneration().resetScript() ? this.editedEmbeddingsGeneration().transforms().map(x => x.Name) : [];
                
        try {
            if (this.enableDocumentExpiration()) {
                await new saveExpirationConfigurationCommand(this.db, {
                    Disabled: false,
                    DeleteFrequencyInSec: null,
                    MaxItemsToProcess: DocumentExpiration.defaultItemsToProcess
                }).execute();

                activeDatabaseTracker.default.database().hasExpirationConfiguration(true);
            }

            const dto = this.editedEmbeddingsGeneration().toDto();
            await saveEtlTaskCommand.forEmbeddingsGeneration(this.db, dto, scriptsToReset).execute();

            this.dirtyFlag().reset();
            this.goToOngoingTasksView();
        } finally {
            this.spinners.save(false)
        }
    }

    cancelOperation() {
        this.goToOngoingTasksView();
    }

    private goToOngoingTasksView() {
        if (this.sourceView() === "AiTasks") {
            router.navigate(appUrl.forAiTasks(this.db));
        } else {
            router.navigate(appUrl.forOngoingTasks(this.db));
        }
    }

    syntaxHelp() {
        const viewmodel = new transformationScriptSyntax("EmbeddingsGeneration");
        app.showBootstrapDialog(viewmodel);
    }

    setState(state: Raven.Client.Documents.Operations.OngoingTasks.OngoingTaskState): void {
        this.editedEmbeddingsGeneration().taskState(state);
    }

    createCollectionNameAutoCompleter() {
        return ko.pureComputed(() => {
            return this.collections().filter(x => !x.isAllDocuments).map(x => x.name);
        });
    }
}

export = editEmbeddingsGenerationTask;
