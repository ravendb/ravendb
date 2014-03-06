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

class shell {
	private router = router;
	databases = ko.observableArray<database>();
	activeDatabase = ko.observable<database>().subscribeTo("ActivateDatabase");
	currentAlert = ko.observable<alertArgs>();
    queuedAlerts = ko.observableArray<alertArgs>();
    databasesLoadedTask: JQueryPromise<any>;
    buildVersion = ko.observable<buildVersionDto>();
    licenseStatus = ko.observable<licenseStatusDto>();
    windowHeightObservable: KnockoutObservable<number>;
    appUrls: computedAppUrls;

    constructor() {
        ko.postbox.subscribe("Alert", (alert: alertArgs) => this.showAlert(alert));
        ko.postbox.subscribe("ActivateDatabaseWithName", (databaseName: string) => this.activateDatabaseWithName(databaseName));
        ko.postbox.subscribe("ActivateDatabase", (db: database) => this.databaseChanged(db));
        
        this.appUrls = appUrl.forCurrentDatabase();

        dynamicHeightBindingHandler.install();
	}

    activate() {
        NProgress.set(.7);
        router.map([
			{ route: ['', 'databases'],	    title: 'Databases',		moduleId: 'viewmodels/databases',		nav: false },
            { route: 'documents',           title: 'Documents',     moduleId: 'viewmodels/documents',       nav: true,  hash: this.appUrls.documents },
		    { route: 'indexes*details',     title: 'Indexes',       moduleId: 'viewmodels/indexesShell',    nav: true,  hash: this.appUrls.indexes },	
            { route: 'query(/:indexName)',	title: 'Query',			moduleId: 'viewmodels/queryShell',		nav: true,  hash: this.appUrls.query(null) },
			{ route: 'tasks*details',	    title: 'Tasks',			moduleId: 'viewmodels/tasks',			nav: true,  hash: this.appUrls.tasks, },
			{ route: 'settings*details',    title: 'Settings',		moduleId: 'viewmodels/settings',		nav: true,  hash: this.appUrls.settings },
            { route: 'status*details',	    title: 'Status',		moduleId: 'viewmodels/status',			nav: true,	hash: this.appUrls.status },
			{ route: 'edit',			    title: 'Edit Document', moduleId: 'viewmodels/editDocument',	nav: false }
        ]).buildNavigationModel();

        // Show progress whenever we navigate.
        router.isNavigating.subscribe(isNavigating => this.showNavigationProgress(isNavigating));

        this.connectToRavenServer();
	}

	// Called by Durandal when shell.html has been put into the DOM.
    attached() {
        // The view must be attached to the DOM before we can hook up keyboard shortcuts.
        jwerty.key("ctrl+alt+n", e => {
			e.preventDefault();
			this.newDocument();
        });

        var dataset: Twitter.Typeahead.Dataset = {
            name: "test",
            local: ["hello","world"]
        };
        $("#goToDocInput").typeahead(dataset);
    }

    handleQuery(query: any, cb: any) {
        debugger;
    }

    showNavigationProgress(isNavigating: boolean) {
        if (isNavigating) {
            NProgress.start();

            var currentProgress = parseFloat(NProgress.status);
            var newProgress = isNaN(currentProgress) ? 0.5 : currentProgress + (currentProgress / 2);
            NProgress.set(newProgress);
        } else {
            NProgress.done();
        }
    }

    databasesLoaded(databases) {
        var systemDatabase = new database("<system>");
        systemDatabase.isSystem = true;
        this.databases(databases.concat([systemDatabase]));
        this.databases()[0].activate();
    }

    launchDocEditor(docId?: string, docsList?: pagedList) {
        var editDocUrl = appUrl.forEditDoc(docId, docsList ? docsList.collectionName : null, docsList ? docsList.currentItemIndex() : null);
        router.navigate(editDocUrl);
    }

	connectToRavenServer() {
        this.databasesLoadedTask = new getDatabasesCommand()
			.execute()
			.fail(result => this.handleRavenConnectionFailure(result))
			.done(results => {
				this.databasesLoaded(results);
                router.activate();
                this.fetchBuildVersion();
                this.fetchLicenseStatus();
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
		var currentAlert = this.currentAlert();
		if (currentAlert) {
			// Maintain a 1000ms time between alerts; otherwise successive alerts can fly by too quickly.
			this.queuedAlerts.push(alert);
			if (currentAlert.type !== alertType.danger) {
				setTimeout(() => this.closeAlertAndShowNext(this.currentAlert()), 1000);
			}
		} else {
			this.currentAlert(alert);
			var fadeTime = 3000;
			if (alert.type === alertType.danger || alert.type === alertType.warning) {
				fadeTime = 5000;
			}
			setTimeout(() => this.closeAlertAndShowNext(alert), fadeTime);
		}
	}

	closeAlertAndShowNext(alertToClose: alertArgs) {
		$('#' + alertToClose.id).alert('close');
		var nextAlert = this.queuedAlerts.pop();
		setTimeout(() => this.currentAlert(nextAlert), 1000); // Give the alert a chance to fade out before we push in the new alert.
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

    databaseChanged(db: database) {
        if (db) {
            new getDatabaseStatsCommand(db)
                .execute()
                .done(result => db.statistics(result));
        }
    }

    selectDatabase(db: database) {
        db.activate();

        var updatedUrl = appUrl.forCurrentPage(db);
        router.navigate(updatedUrl);
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
}

export = shell;