import viewModelBase = require("viewmodels/viewModelBase");
import migrateDatabaseCommand = require("commands/database/studio/migrateDatabaseCommand");
import migrateDatabaseModel = require("models/database/tasks/migrateDatabaseModel");
import notificationCenter = require("common/notifications/notificationCenter");
import eventsCollector = require("common/eventsCollector");
import aceEditorBindingHandler = require("common/bindingHelpers/aceEditorBindingHandler");
import defaultAceCompleter = require("common/defaultAceCompleter");
import popoverUtils = require("common/popoverUtils");
import generalUtils = require("common/generalUtils");
import recentError = require("common/notifications/models/recentError");
import viewHelpers = require("common/helpers/view/viewHelpers");
import lastUsedAutocomplete = require("common/storage/lastUsedAutocomplete");

class migrateDatabase extends viewModelBase {

    model = new migrateDatabaseModel();
    completer = defaultAceCompleter.completer();
    
    submitButtonEnabled: KnockoutComputed<boolean>;
    submitButtonText: KnockoutComputed<string>;
    
    databaseNameHasFocus = ko.observable<boolean>(false);
    
    migratorPathAutocomplete: lastUsedAutocomplete;

    spinners = {
        getDatabaseNames: ko.observable<boolean>(false),
        getCollectionNames: ko.observable<boolean>(false),
        migration: ko.observable<boolean>(false)
    };

    constructor() {
        super();

        // please notice autocomplete instance is shared across all databases
        this.migratorPathAutocomplete = new lastUsedAutocomplete("migrator-path", this.model.migratorFullPath);
        
        aceEditorBindingHandler.install();
        this.initObservables();
        this.initValidation();
        
        this.databaseNameHasFocus.subscribe(focus => {
            if (focus) {
                const configuration = this.model.activeConfiguration();
                
                configuration.databaseNames([]);
                
                viewHelpers.asyncValidationCompleted(this.model.validationGroupDatabaseNames, () => {
                   if (this.isValid(this.model.validationGroupDatabaseNames)) {
                       this.getDatabases();
                   }
                });
            }
        });
        
        const getCollectionsDebounced = _.debounce(() => this.getCollections(), 500);
        
        const mongoConfiguration = this.model.mongoDbConfiguration;
        mongoConfiguration.connectionString.subscribe(() => getCollectionsDebounced());
        mongoConfiguration.databaseName.subscribe(() => getCollectionsDebounced());
        
        const cosmosConfiguration = this.model.cosmosDbConfiguration;
        cosmosConfiguration.azureEndpointUrl.subscribe(() => getCollectionsDebounced());
        cosmosConfiguration.primaryKey.subscribe(() => getCollectionsDebounced());
        cosmosConfiguration.databaseName.subscribe(() => getCollectionsDebounced());
    }
    
    private initObservables() {
        this.submitButtonText = ko.pureComputed(() => {
            const configuration = this.model.activeConfiguration();
            if (configuration && !configuration.migrateAllCollections()) {
                return `Migrate ${this.pluralize(configuration.selectedCollectionsCount(), 'collecton', 'collections')}`;
            }

            return "Migrate all collections";
        });

        this.submitButtonEnabled = ko.pureComputed(() => {
            const configuration = this.model.activeConfiguration();
            if (configuration && !configuration.migrateAllCollections()) {
                return configuration.selectedCollectionsCount() > 0;
            }

            return true;
        });
    }
    
    private initValidation() {
        const checkMigratorFullPath = (val: string, params: any, callback: (currentValue: string, result: string | boolean) => void) => {
            migrateDatabaseCommand.validateMigratorPath(this.activeDatabase(), this.model.migratorFullPath())
                .execute()
                .done(() => {
                    callback(this.model.migratorFullPath(), true);
                })
                .fail((response: JQueryXHR) => {
                    const messageAndOptionalException = recentError.tryExtractMessageAndException(response.responseText);
                    callback(this.model.migratorFullPath(), messageAndOptionalException.message);
                })
            };
        
        this.model.migratorFullPath.extend({
            required: true,
            validation: {
                async: true,
                validator: generalUtils.debounceAndFunnel(checkMigratorFullPath)
            }
        });
    }

    compositionComplete() {
        super.compositionComplete();

        popoverUtils.longWithHover($(".migrator-path small"),
            {
                content: '<strong>Raven.Migrator.exe</strong> can be found in <strong>tools</strong><br /> package (for version v4.x) on <a target="_blank" href="http://ravendb.net/downloads">ravendb.net</a> website'
            });

        popoverUtils.longWithHover($(".migrate-gridfs small"),
            {
                content: 'GridFS attachments will be saved as documents with attachments in <strong>@files</strong> collection.'
            });

        popoverUtils.longWithHover($("#scriptPopover"),
            {
                content:
                    "<div class=\"text-center\">Transform scripts are written in JavaScript </div>" +
                        "<pre><span class=\"token keyword\">var</span> name = <span class=\"token keyword\">this.</span>FirstName;<br />" +
                        "<span class=\"token keyword\">if</span> (name === <span class=\"token string\">'Bob'</span>)<br />&nbsp;&nbsp;&nbsp;&nbsp;" +
                        "<span class=\"token keyword\">throw </span><span class=\"token string\">'skip'</span>; <span class=\"token comment\">// filter-out</span><br /><br />" +
                        "<span class=\"token keyword\">this</span>.Freight = <span class=\"token number\">15.3</span>;<br />" +
                        "</pre>"
            });
        
        this.initInlineEdit();
    }
    
    private initInlineEdit() {
        const $body = $("body");

        this.registerDisposableHandler($body, "click", (event: JQueryEventObject) => {
            if ($(event.target).closest(".inline-edit").length === 0) {
                // click outside edit area - close all of them

                $(".inline-edit.edit-mode")
                    .removeClass("edit-mode");
            }
        });

        $(".migrate-database").on("click", ".inline-edit", event => {
            event.preventDefault();

            $(".inline-edit.edit-mode")
                .removeClass("edit-mode");

            const container = $(event.target).closest(".inline-edit");
            if (!container.hasClass("edit-disabled")) {
                container.addClass("edit-mode");
                $("input", container).focus();
            }
        });
    }

    private getDatabases() {
        const activeConfiguration = this.model.activeConfiguration();

        this.spinners.getDatabaseNames(true);
        const selectMigrationOption = this.model.selectMigrationOption();
        const db = this.activeDatabase();

        return migrateDatabaseCommand.getDatabaseNames(db, this.model.toDto())
            .execute()
            .done((databasesInfo) => {
                activeConfiguration.databaseNames(databasesInfo.Databases);
            })
            .fail(() => {
                if (selectMigrationOption === "MongoDB") {
                    this.model.mongoDbConfiguration.hasGridFs(false);
                }
            })
            .always(() => this.spinners.getDatabaseNames(false));
    }

    getCollections() {
        const activeConfiguration = this.model.activeConfiguration();
        if (!this.isValid(this.model.validationGroup) || !activeConfiguration) {
            return;
        }

        this.spinners.getCollectionNames(true);
        const db = this.activeDatabase();
        const selectMigrationOption = this.model.selectMigrationOption();
        
        migrateDatabaseCommand.getCollections(db, this.model.toDto())
            .execute()
            .done((collectionInfo) => {
                if (selectMigrationOption !== this.model.selectMigrationOption()) {
                    return;
                }

                activeConfiguration.setCollections(collectionInfo.Collections);
                if (selectMigrationOption === "MongoDB") {
                    this.model.mongoDbConfiguration.hasGridFs(collectionInfo.HasGridFS);
                }
            })
            .fail(() => {
                activeConfiguration.setCollections([]);
                if (selectMigrationOption === "MongoDB") {
                    this.model.mongoDbConfiguration.hasGridFs(false);
                }
            })
            .always(() => this.spinners.getCollectionNames(false));
    }
    
    migrateDb() {
        viewHelpers.asyncValidationCompleted(this.model.validationGroup, () => {
            if (!this.isValid(this.model.validationGroup)) {
                return;
            }

            this.migratorPathAutocomplete.recordUsage();

            eventsCollector.default.reportEvent("database", "migrate");
            this.spinners.migration(true);

            const db = this.activeDatabase();

            migrateDatabaseCommand.migrate(db, this.model.toDto())
                .execute()
                .done((operationIdDto: operationIdDto) => {
                    const operationId = operationIdDto.OperationId;
                    notificationCenter.instance.openDetailsForOperationById(db, operationId);
                })
                .always(() => this.spinners.migration(false));
        });
    }
}

export = migrateDatabase; 
