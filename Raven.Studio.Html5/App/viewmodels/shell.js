/// <reference path="../../Scripts/typings/nprogress/nprogress.d.ts" />
/// <reference path="../../Scripts/typings/bootstrap/bootstrap.d.ts" />
define(["require", "exports", "plugins/router", "durandal/app", "durandal/system", "models/database", "common/appUrl", "common/alertType", "commands/getDatabaseStatsCommand", "commands/getDatabasesCommand", "commands/getBuildVersionCommand", "commands/getLicenseStatusCommand"], function(require, exports, router, app, sys, database, appUrl, alertType, getDatabaseStatsCommand, getDatabasesCommand, getBuildVersionCommand, getLicenseStatusCommand) {
    var shell = (function () {
        function shell() {
            var _this = this;
            this.router = router;
            this.databases = ko.observableArray();
            this.activeDatabase = ko.observable().subscribeTo("ActivateDatabase");
            this.currentAlert = ko.observable();
            this.queuedAlerts = ko.observableArray();
            this.buildVersion = ko.observable();
            this.licenseStatus = ko.observable();
            ko.postbox.subscribe("Alert", function (alert) {
                return _this.showAlert(alert);
            });
            ko.postbox.subscribe("ActivateDatabaseWithName", function (databaseName) {
                return _this.activateDatabaseWithName(databaseName);
            });
            ko.postbox.subscribe("ActivateDatabase", function (db) {
                return _this.databaseChanged(db);
            });

            //ko.postbox.subscribe("EditDocument", args => this.launchDocEditor(args.doc.getId(), args.docsList));
            NProgress.set(.5);
        }
        shell.prototype.activate = function () {
            NProgress.set(.8);
            router.map([
                { route: ['', 'databases'], title: 'Databases', moduleId: 'viewmodels/databases', nav: false },
                { route: 'documents', title: 'Documents', moduleId: 'viewmodels/documents', nav: true, hash: appUrl.forCurrentDatabase().documents },
                { route: 'indexes', title: 'Indexes', moduleId: 'viewmodels/indexes', nav: true },
                { route: 'query', title: 'Query', moduleId: 'viewmodels/query', nav: true },
                { route: 'tasks', title: 'Tasks', moduleId: 'viewmodels/tasks', nav: true },
                { route: 'settings*details', title: 'Settings', moduleId: 'viewmodels/settings', nav: true, hash: appUrl.forCurrentDatabase().settings },
                { route: 'status*details', title: 'Status', moduleId: 'viewmodels/status', nav: true, hash: appUrl.forCurrentDatabase().status },
                { route: 'edit', title: 'Edit Document', moduleId: 'viewmodels/editDocument', nav: false }
            ]).buildNavigationModel();

            // Show progress whenever we navigate.
            router.isNavigating.subscribe(function (isNavigating) {
                if (isNavigating) {
                    NProgress.start();
                    NProgress.set(.5);
                } else {
                    NProgress.done();
                }
            });

            this.connectToRavenServer();
        };

        // The view must be attached to the DOM before we can hook up keyboard shortcuts.
        shell.prototype.attached = function () {
            var _this = this;
            jwerty.key("ctrl+alt+n", function (e) {
                e.preventDefault();
                _this.newDocument();
            });
        };

        shell.prototype.databasesLoaded = function (databases) {
            var systemDatabase = new database("<system>");
            systemDatabase.isSystem = true;
            this.databases(databases.concat([systemDatabase]));
            this.databases()[0].activate();
        };

        shell.prototype.launchDocEditor = function (docId, docsList) {
            var editDocUrl = appUrl.forEditDoc(docId, docsList ? docsList.collectionName : null, docsList ? docsList.currentItemIndex() : null);
            router.navigate(editDocUrl);
        };

        shell.prototype.connectToRavenServer = function () {
            var _this = this;
            this.databasesLoadedTask = new getDatabasesCommand().execute().fail(function (result) {
                return _this.handleRavenConnectionFailure(result);
            }).done(function (results) {
                _this.databasesLoaded(results);
                router.activate();
                _this.fetchBuildVersion();
                _this.fetchLicenseStatus();
            });
        };

        shell.prototype.handleRavenConnectionFailure = function (result) {
            var _this = this;
            NProgress.done();
            sys.log("Unable to connect to Raven.", result);
            var tryAgain = 'Try again';
            var messageBoxResultPromise = app.showMessage("Couldn't connect to Raven. Details in the browser console.", ":-(", [tryAgain]);
            messageBoxResultPromise.done(function (messageBoxResult) {
                if (messageBoxResult === tryAgain) {
                    NProgress.start();
                    _this.connectToRavenServer();
                }
            });
        };

        shell.prototype.showAlert = function (alert) {
            var _this = this;
            var currentAlert = this.currentAlert();
            if (currentAlert) {
                // Maintain a 500ms time between alerts; otherwise successive alerts can fly by too quickly.
                this.queuedAlerts.push(alert);
                if (currentAlert.type !== 3 /* danger */) {
                    setTimeout(function () {
                        return _this.closeAlertAndShowNext(_this.currentAlert());
                    }, 500);
                }
            } else {
                this.currentAlert(alert);
                var fadeTime = 3000;
                if (alert.type === 3 /* danger */ || alert.type === 2 /* warning */) {
                    fadeTime = 5000;
                }
                setTimeout(function () {
                    return _this.closeAlertAndShowNext(alert);
                }, fadeTime);
            }
        };

        shell.prototype.closeAlertAndShowNext = function (alertToClose) {
            var _this = this;
            $('#' + alertToClose.id).alert('close');
            var nextAlert = this.queuedAlerts.pop();
            setTimeout(function () {
                return _this.currentAlert(nextAlert);
            }, 500); // Give the alert a chance to fade out before we push in the new alert.
        };

        shell.prototype.newDocument = function () {
            this.launchDocEditor(null);
        };

        shell.prototype.activateDatabaseWithName = function (databaseName) {
            var _this = this;
            if (this.databasesLoadedTask) {
                this.databasesLoadedTask.done(function () {
                    var matchingDatabase = _this.databases().first(function (d) {
                        return d.name == databaseName;
                    });
                    if (matchingDatabase && _this.activeDatabase() !== matchingDatabase) {
                        ko.postbox.publish("ActivateDatabase", matchingDatabase);
                    }
                });
            }
        };

        shell.prototype.databaseChanged = function (db) {
            if (db) {
                new getDatabaseStatsCommand(db).execute().done(function (result) {
                    return db.statistics(result);
                });
            }
        };

        shell.prototype.fetchBuildVersion = function () {
            var _this = this;
            new getBuildVersionCommand().execute().done(function (result) {
                return _this.buildVersion(result);
            });
        };

        shell.prototype.fetchLicenseStatus = function () {
            var _this = this;
            new getLicenseStatusCommand().execute().done(function (result) {
                return _this.licenseStatus(result);
            });
        };
        return shell;
    })();

    
    return shell;
});
//# sourceMappingURL=shell.js.map
