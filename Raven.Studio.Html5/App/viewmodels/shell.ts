/// <reference path="../../Scripts/typings/nprogress/nprogress.d.ts" />
/// <reference path="../../Scripts/typings/bootstrap/bootstrap.d.ts" />

import router = require("plugins/router");
import app = require("durandal/app");
import sys = require("durandal/system");

import database = require("models/database");
import document = require("models/document");
import appUrl = require("common/appUrl");
import collection = require("models/collection");
import deleteDocuments = require("viewmodels/deleteDocuments");
import dialogResult = require("common/dialogResult");
import alertArgs = require("common/alertArgs");
import alertType = require("common/alertType");
import pagedList = require("common/pagedList");
import getDatabaseStatsCommand = require("commands/getDatabaseStatsCommand");
import getDatabasesCommand = require("commands/getDatabasesCommand");
import getBuildVersionCommand = require("commands/getBuildVersionCommand");
import getLicenseStatusCommand = require("commands/getLicenseStatusCommand");
import dynamicHeightBindingHandler = require("common/dynamicHeightBindingHandler");
import viewModelBase = require("viewmodels/viewModelBase");
import getDocumentsMetadataByIDPrefixCommand = require("commands/getDocumentsMetadataByIDPrefixCommand");
import viewSystemDatabaseConfirm = require("viewmodels/viewSystemDatabaseConfirm");
import getDocumentWithMetadataCommand = require("commands/getDocumentWithMetadataCommand");

class shell extends viewModelBase {
    private router = router;
    databases = ko.observableArray<database>();
    currentAlert = ko.observable<alertArgs>();
    queuedAlert: alertArgs;
    databasesLoadedTask: JQueryPromise<any>;
    buildVersion = ko.observable<buildVersionDto>();
    licenseStatus = ko.observable<licenseStatusDto>();
    windowHeightObservable: KnockoutObservable<number>;
    appUrls: computedAppUrls;
    recordedErrors = ko.observableArray<alertArgs>();
    newIndexUrl = appUrl.forCurrentDatabase().newIndex;
    newTransformerUrl = appUrl.forCurrentDatabase().newTransformer;
    isFirstModelPoll: boolean;


    DocumentPrefix = ko.observable<String>();
    

    constructor() {
        super();
        this.isFirstModelPoll = true;
        ko.postbox.subscribe("Alert", (alert: alertArgs) => this.showAlert(alert));
        ko.postbox.subscribe("ActivateDatabaseWithName", (databaseName: string) => this.activateDatabaseWithName(databaseName));

        this.appUrls = appUrl.forCurrentDatabase();

        dynamicHeightBindingHandler.install();
    }

    activate(args: any) {
        super.activate(args);

        NProgress.set(.7);
        router.map([
            { route: ['', 'databases'], title: 'Databases', moduleId: 'viewmodels/databases', nav: false, hash: this.appUrls.databasesManagement },
            { route: 'documents', title: 'Documents', moduleId: 'viewmodels/documents', nav: true, hash: this.appUrls.documents },
            { route: 'indexes*details', title: 'Indexes', moduleId: 'viewmodels/indexesShell', nav: true, hash: this.appUrls.indexes },
            { route: 'transformers*details', title: 'Transformers', moduleId: 'viewmodels/transformersShell', nav: false, hash: this.appUrls.transformers },
            { route: 'query*details', title: 'Query', moduleId: 'viewmodels/queryShell', nav: true, hash: this.appUrls.query(null) },
            { route: 'tasks*details', title: 'Tasks', moduleId: 'viewmodels/tasks', nav: true, hash: this.appUrls.tasks, },
            { route: 'settings*details', title: 'Settings', moduleId: 'viewmodels/settings', nav: true, hash: this.appUrls.settings },
            { route: 'status*details', title: 'Status', moduleId: 'viewmodels/status', nav: true, hash: this.appUrls.status },
            { route: 'edit', title: 'Edit Document', moduleId: 'viewmodels/editDocument', nav: false }
        ]).buildNavigationModel();

        // Show progress whenever we navigate.
        router.isNavigating.subscribe(isNavigating => this.showNavigationProgress(isNavigating));

        this.router.guardRoute = (instance: Object, instruction: DurandalRouteInstruction) => this.getValidRoute(instance, instruction);
        
        this.connectToRavenServer();
    }

    getValidRoute(instance: Object, instruction: DurandalRouteInstruction) :any {
        if (appUrl.warnWhenUsingSystemDatabase && !this.activeDatabase().isSystem && instruction.queryString && (instruction.queryString.indexOf("database=<system>") >= 0 || instruction.queryString.indexOf("database=%3Csystem%3E") >= 0 )) {
            var systemDbConfirm = new viewSystemDatabaseConfirm(this.activeDatabase());

            systemDbConfirm.viewTask.done(()=> {
                var systemDb = appUrl.getSystemDatabase();
                systemDb.activate();

                var lastRoute = appUrl.forCurrentPage(systemDb);

                if (!lastRoute) {
                    if (window.location.hash.indexOf("database") < 0) {
                        lastRoute = window.location.hash + "?database=" + encodeURIComponent(systemDb.name);
                    } else {
                        lastRoute = window.location.hash;
                        lastRoute.substring(lastRoute.length - 2, lastRoute.length) == "&&" ? lastRoute = lastRoute.replace("&&", "&") : lastRoute = lastRoute + "&";
                    }

                }
                else if (lastRoute.indexOf("database") < 0) {
                    lastRoute = lastRoute + "?database=" + encodeURIComponent(systemDb.name);
                }
                
                router.navigate(lastRoute);

            }).fail((lastDb:database) => {
                var lastRoute = appUrl.forCurrentPage(lastDb);

                if (!lastRoute) {
                    if (window.location.hash.indexOf("database") < 0) {
                        lastRoute = window.location.hash + "?database=" + encodeURIComponent(lastDb.name);
                    } else {
                        lastRoute = window.location.hash.replace("database=%3Csystem%3E", "database=" + encodeURIComponent(lastDb.name));
                    } 
                }
                else if (lastRoute.indexOf("database") < 0) {
                    lastRoute = lastRoute + "?database=" + encodeURIComponent(lastDb.name);
                }

                router.navigate(lastRoute ? lastRoute : window.location.hash);
                
            });
            app.showDialog(systemDbConfirm);
            return false;
        } else {
            return true;
        }
    }


    // Called by Durandal when shell.html has been put into the DOM.
    attached() {
        // The view must be attached to the DOM before we can hook up keyboard shortcuts.
        jwerty.key("ctrl+alt+n", e=> {
            e.preventDefault();
            this.newDocument();
        });

        $("body").tooltip({
            delay: { show: 1000, hide: 100 },
            container: 'body',
            selector: '.use-bootstrap-tooltip',
            trigger: 'hover'
        });
        
        //TODO: Move this to a knockout binding handler
        $("#goToDocInput").typeahead(
            {
                hint: true,
                highlight: true,
                minLength: 1
            },
            {
                name: 'Documents',
                displayKey: 'value',
                source: (searchTerm, callback) => {
                    var foundDocuments;
                    new getDocumentsMetadataByIDPrefixCommand(searchTerm, 25, this.activeDatabase())
                        .execute()
                        .done((results: string[]) => {
                            var matches = results.map(val => {
                                return {
                                    value: val,
                                    editHref: appUrl.forEditDoc(val, null, null, this.activeDatabase())
                                }
                            });
                            callback(matches);
                        })
                        .fail(callback(['']));

                },
                templates: {
                    suggestion: Handlebars.compile(['<p><a><strong>{{value}}</a></strong>'].join())
                }


            });


        $('#goToDocInput').bind('typeahead:selected', (obj, datum, name) => {
            router.navigate(datum.editHref);
        });

    }

    showNavigationProgress(isNavigating: boolean) {
        if (isNavigating) {
            NProgress.start();

            var currentProgress = parseFloat(NProgress.status);
            var newProgress = isNaN(currentProgress) ? 0.5 : currentProgress + (currentProgress / 2);
            NProgress.set(newProgress);
        } else {
            NProgress.done();
            $('.use-bootstrap-tooltip').tooltip('hide');
        }
    }

    databasesLoaded(databases) {
        var systemDatabase = new database("<system>");
        systemDatabase.isSystem = true;
        systemDatabase.isVisible(false);
        this.databases(databases.concat([systemDatabase]));
        if (this.databases().length == 1) {
            systemDatabase.activate();
        } else {
            this.databases.first(x=> x.isVisible()).activate();
        }
        
    }

    launchDocEditor(docId?: string, docsList?: pagedList) {
        var editDocUrl = appUrl.forEditDoc(docId, docsList ? docsList.collectionName : null, docsList ? docsList.currentItemIndex() : null, this.activeDatabase());
        this.navigate(editDocUrl);
    }

    connectToRavenServer() {
        this.databasesLoadedTask = new getDatabasesCommand()
            .execute()
            .fail(result => this.handleRavenConnectionFailure(result))
            .done(results => {
                this.databasesLoaded(results);
            new getDocumentWithMetadataCommand("Raven/StudioConfig", appUrl.getSystemDatabase()).
                execute()
                .done((doc: document)=> {
                    if (!doc["WarnWhenUsingSystemDatabase"] && doc["WarnWhenUsingSystemDatabase"] == false) {
                        appUrl.warnWhenUsingSystemDatabase = false;
                    }
                }).always(()=> {
                    router.activate();
                    this.fetchBuildVersion();
                    this.fetchLicenseStatus();
                });
        });
    }

    handleRavenConnectionFailure(result) {
        NProgress.done();
        sys.log("Unable to connect to Raven.", result);
        var tryAgain = 'Try again';
        var messageBoxResultPromise = app.showMessage("Couldn't connect to Raven. Details in the browser console.", ":-(", [tryAgain]);
        messageBoxResultPromise.done(messageBoxResult => {
            if (messageBoxResult === tryAgain) {
                NProgress.start();
                this.connectToRavenServer();
            }
        });
    }

    showAlert(alert: alertArgs) {
        if (alert.type === alertType.danger || alert.type === alertType.warning) {
            this.recordedErrors.unshift(alert);
        }

        var currentAlert = this.currentAlert();
        if (currentAlert) {
            this.queuedAlert = alert;
            this.closeAlertAndShowNext(currentAlert);
        } else {
            this.currentAlert(alert);
            var fadeTime = 2000; // If there are no pending alerts, show it for 2 seconds before fading out.
            if (alert.type === alertType.danger || alert.type === alertType.warning) {
                fadeTime = 4000; // If there are no pending alerts, show the error alert for 4 seconds before fading out.
            }
            setTimeout(() => this.closeAlertAndShowNext(alert), fadeTime);
        }
    }

    closeAlertAndShowNext(alertToClose: alertArgs) {
        var alertElement = $('#' + alertToClose.id);
        if (alertElement.length === 0) {
            return;
        }

        // If the mouse is over the alert, keep it around.
        if (alertElement.is(":hover")) {
            setTimeout(() => this.closeAlertAndShowNext(alertToClose), 1000);
        } else {
            alertElement.alert('close');
        }
    }

    onAlertHidden() {
        this.currentAlert(null);
        var nextAlert = this.queuedAlert;
        if (nextAlert) {
            this.queuedAlert = null;
            this.showAlert(nextAlert);
        }
    }

    newDocument() {
        this.launchDocEditor(null);
    }

    activateDatabaseWithName(databaseName: string) {
        if (this.databasesLoadedTask) {
            this.databasesLoadedTask.done(() => {
                var matchingDatabase = this.databases().first(d => d.name == databaseName);
                if (matchingDatabase && this.activeDatabase() !== matchingDatabase) {
                    ko.postbox.publish("ActivateDatabase", matchingDatabase);
                }
            });
        }
    }

    modelPolling() {
        new getDatabasesCommand()
            .execute()
            .done(results => {
                ko.utils.arrayForEach(results, (result:database) => {
                    var existingDb = this.databases().first(d=> {
                        return d.name == result.name;
                    });
                if (!existingDb ) {
                    this.databases.unshift(result);
                    }
                
                });

        });

        var db = this.activeDatabase();
        if (db) {
            new getDatabaseStatsCommand(db)
                .execute()
                .done(result=> db.statistics(result));
        }
    }

    selectDatabase(db: database) {
        if (db.name != this.activeDatabase().name) {
            db.activate();
            var updatedUrl = appUrl.forCurrentPage(db);
            this.navigate(updatedUrl);
        }
    }

    fetchBuildVersion() {
        new getBuildVersionCommand()
            .execute()
            .done((result: buildVersionDto) => this.buildVersion(result));
    }

    fetchLicenseStatus() {
        new getLicenseStatusCommand()
            .execute()
            .done((result: licenseStatusDto) => this.licenseStatus(result));
    }

    showErrorsDialog() {
        require(["viewmodels/recentErrors"], ErrorDetails => {
            var dialog = new ErrorDetails(this.recordedErrors);
            app.showDialog(dialog);
        });
    }
}

export = shell;