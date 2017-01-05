/// <reference path="../../../src/Raven.Studio/typings/tsd.d.ts" />

import Squire = require("Squire");
import activator = require("durandal/activator");
import extensions = require("src/Raven.Studio/typescript/common/extensions");
import system = require("durandal/system");
import composition = require("durandal/composition");
import binder = require("durandal/binder");

import ace = require("ace/ace");

system.debug(true);

type dbCreator = (db: new (dto: Raven.Client.Data.DatabaseInfo) => any) => any;

type viewmodelTestOpts<T> = {
    initViewmodel?: (vm: T) => void;
    afterAttach?: (vm: T) => void;
    afterCtr?: (vm: T) => void;
    afterComposition?: (vm: T) => void;
    assertions?: (vm: T, $container: JQuery) => void;
    activateArgs?: () => any;
};

class Utils { 

    static errorHolder = [] as string[];

    static injector = new Squire();

    static readonly viewModelPrefix = "src/Raven.Studio/typescript/viewmodels/";
    static readonly viewTemplatePrefix = "src/Raven.Studio/wwwroot/App/views/";
    static readonly viewTemplateSuffix = ".html";

    static initTest() {
        Utils.initInjector();
    }

    static mockActiveDatabase(factory?: dbCreator): Promise<void> {
        factory = factory || (x => new x(Utils.databaseNamed("default")));

        return new Promise<void>((resolve, reject) => {
            Utils.injector.require(["models/resources/database", "common/shell/activeResourceTracker"], (dbCtr: new () => any, resourceTracker: any) => {
                var dbInstance = factory(dbCtr);
                resourceTracker.default.resource(dbInstance);
                resolve();
            }, reject);
        });
    }

    static databaseNamed(dbName: string) {
        return {
            Name: dbName,
            Disabled: false
        } as Raven.Client.Data.DatabaseInfo;
    }

    static initInjector() {
        extensions.install();

        beforeEach(() => {

            Utils.injector = new Squire();
            Utils.injector
                .mock('knockout', ko)
                .mock('jquery', jQuery);

            Utils.mockCommand('commands/auth/getSingleAuthTokenCommand', () => ({ Token: "Fake Token" }));
            Utils.mockCommand('commands/resources/getResourcesCommand', () => ({ "Databases": [] } as Raven.Client.Data.ResourcesInfo));

            return this.aceEditorFacade(Utils.injector)
                .then(() => Utils.applyConfiguration());
        });

        afterEach(() => {
            Utils.injector.remove();
            Utils.injector = null;
        });
    }

    private static applyConfiguration() {
        return Promise.all([
            Utils.configureOAuthContext(),
            Utils.configureMockWebSocket(),
            Utils.configureCommandBase(),
            Utils.configureViewModelBase(),
            Utils.configureAceEditorHandler(),
        ]);
    }

    private static configureOAuthContext() {
        return Utils.requireAndConfigure("common/oauthContext", context => {
            context.enterApiKeyTask = $.Deferred().resolve();
        });
    }

    private static configureCommandBase() {
        return Utils.requireAndConfigure("commands/commandBase", commandBase => {
            commandBase.prototype.ajax = function () {
                const errorMsg = "Command execution is not supported during tests at: " + (<any>this).__moduleId__;
                Utils.errorHolder.push(errorMsg);
                throw new Error(errorMsg);
            }
        });
    }

    private static configureMockWebSocket() {
        return Utils.requireAndConfigure("common/abstractWebSocketClient", webSocket => {
            webSocket.prototype.connectWebSocket = function () {
                setTimeout(() => this.onOpen(), 0);
            }
        });
    }

    private static configureAceEditorHandler() {
        return Utils.requireAndConfigure("common/bindingHelpers/aceEditorBindingHandler", aceEditorBindingHandler => {
            aceEditorBindingHandler.useWebWorkers = false;
        });
    }

    private static configureViewModelBase() {
        return Utils.requireAndConfigure("viewmodels/viewModelBase", viewModelBase => {
            viewModelBase.prototype.getPageHostDimenensions = () => [1500, 500];
        });
    }

    static requireAndConfigure(moduleName: string, configuration: (obj: any) => void) {
        return new Promise<void>((resolve, reject) => {
            Utils.injector
                .require([moduleName], (obj: any) => {
                    try {
                        configuration(obj);
                        resolve();
                    } catch (e) {
                        reject(e);
                    }
                }, reject);
        });
    }

    private static aceEditorFacade(squire: Squire): Promise<void> {
        squire.mock("ace/ace", ace); // needed for global ace preload

        var deps: Array<string> = [];

        var globalDefines = (<any>requirejs).s.contexts._.defined;
        for (var def in globalDefines) {
            if (globalDefines.hasOwnProperty(def)) {
                if (def.startsWith("ace/")) {
                    squire.mock(def, globalDefines[def]);
                    deps.push(def);
                }
            }
        }
        return new Promise<void>((resolve, reject) => squire.require(deps, resolve, reject));
    }

    static mockCommand<T>(commandName: string, resultProvider: () => T): void {
        Utils.injector.clean(commandName);
        Utils.injector.mock(commandName, () => ({
            execute: () => $.Deferred<T>().resolve(resultProvider(), "OK", null)
        }));
    }

    private static cleanup(activatorInstance: DurandalActivator<any>, $test: JQuery): void {
        activatorInstance(null);
        const child = $test[0].children[0];
        if (child) {
            ko.cleanNode(child);    
        }
        
        $test.html("");
    }

    static runViewmodelTest<T>(viewModelName: string, opts: viewmodelTestOpts<T>): Promise<void> {
        var activatorInstance = activator.create();
        var $test = $("#test");

        var testPromise = new Promise<void>((resolve, reject) => {
            Utils.injector.
                require([Utils.viewModelPrefix + viewModelName], (viewModelCtr: new () => T) => {
                    var vm = new viewModelCtr();

                    if (opts.afterCtr) {
                        opts.afterCtr(vm);
                    }

                    var activationData = opts.activateArgs ? opts.activateArgs() : undefined;

                    activatorInstance.activateItem(vm, activationData).then((result: boolean) => {
                        if (!result) {
                            const queue = Utils.errorHolder;
                            Utils.errorHolder = [];
                            const joinedErrors = queue.join(", ");

                            reject(new Error('unable to activate item: ' + joinedErrors));
                            return;
                        }
                        if (opts.initViewmodel) {
                            opts.initViewmodel(vm);
                        }

                        binder.throwOnErrors = true;

                        Utils.composeViewForTest<T>(vm, opts, viewModelName, resolve, reject);
                            
                    }).fail(err => reject(err));
                }, reject);
        });

        return testPromise
            .then(() => Utils.cleanup(activatorInstance, $test))
            .catch(err => {
                Utils.cleanup(activatorInstance, $test);
                throw err;
            });
    }

    private static composeViewForTest<T>(vm: T, opts: viewmodelTestOpts<T>, viewModelName: string, resolve: Function, reject: Function) {
        var $test = $("#test");

        composition.compose($test[0], <any>{
            activate: false, // we use external activator 
            cacheViews: false,
            model: vm,
            view: Utils.viewTemplatePrefix + viewModelName + Utils.viewTemplateSuffix,
            attached: () => {
                if (opts.afterAttach) {
                    opts.afterAttach(vm);
                }
            },
            onError: reject,
            compositionComplete: () => {
                if (opts.afterComposition) {
                    opts.afterComposition(vm);
                }
                setTimeout(() => {
                    if (opts.assertions) {
                        opts.assertions(vm, $test);
                    }
                    resolve();
                });
            }
        }, null);
    }
}

export = Utils;
