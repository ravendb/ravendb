/// <reference path="../../../src/Raven.Studio/typings/tsd.d.ts" />

import Squire = require("Squire");
import activator = require("durandal/activator");
import extensions = require("src/Raven.Studio/typescript/common/extensions");
import commandBaseMock = require("mocks/commandBaseMock");
import system = require("durandal/system");
import composition = require("durandal/composition");
import binder = require("durandal/binder");

import database = require("src/Raven.Studio/typescript/models/resources/database");
import ace = require("ace/ace");

system.debug(true);

type dbCreator = (db: new (name: string, isAdminCurrentTenant?: boolean, isDisabled?: boolean, bundles?: string[], isIndexingDisabled?: boolean, isRejectClientsMode?: boolean, isLoaded?: boolean, clusterWide?: boolean) => database) => database;

type viewmodelTestOpts<T> = {
    initViewmodel?: (vm: T) => void;
    afterAttach?: (vm: T) => void;
    afterComposition?: (vm: T) => void;
    assertions?: (vm: T, $container: JQuery) => void;
};

class Utils { 

    static injector = new Squire();

    static viewModelPrefix = "src/Raven.Studio/typescript/viewmodels/";
    static viewTemplatePrefix = "src/Raven.Studio/wwwroot/App/views/";
    static viewTemplateSuffix = ".html";

    static initTest() {
        Utils.initInjector();
    }

    static mockActiveDatabase(factory: dbCreator): Promise<void> {
        return new Promise<void>((resolve, reject) => {
            Utils.injector.require(["models/resources/database", "viewmodels/resources/activeResourceTracker"], (dbCtr: new () => database, resourceTracker: any) => {
                var dbInstance = factory(dbCtr);
                resourceTracker.default.resource(dbInstance);
                resolve();
            }, reject);
        });
    }

    static initInjector() {
        extensions.install();

        beforeEach(() => {

            Utils.injector = new Squire();
            Utils.injector
                .mock('knockout', ko)
                .mock('jquery', jQuery)
                .mock('commands/commandBase', commandBaseMock);

            return this.aceEditorFacade(Utils.injector)
                .then(() => new Promise<void>((resolve, reject) => {
                    Utils.injector.store(['common/oauthContext'])
                        .require(["common/oauthContext", "common/bindingHelpers/aceEditorBindingHandler"], (context: any, aceEditorBindingHandler: any) => {
                            context.enterApiKeyTask = $.Deferred().resolve();
                            aceEditorBindingHandler.useWebWorkers = false;

                            resolve();
                        }, reject);
                }));
        });

        afterEach(() => {
            Utils.injector.remove();
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

    static mockCommand<T>(commandName: string, resultProvider:() => T): void {
        Utils.injector.mock(commandName, () => ({
            execute: () => $.Deferred<T>().resolve(resultProvider())
        }));
    }

    private static cleanup(activatorInstance: DurandalActivator<any>, $test: JQuery): void {
        activatorInstance(null);
        ko.cleanNode($test[0].children[0]);
        $test.html("");
    }

    static runViewmodelTest<T>(viewModelName: string, opts: viewmodelTestOpts<T>): Promise<void> {
        var activatorInstance = activator.create();
        var $test = $("#test");

        var testPromise = new Promise<void>((resolve, reject) => {
            Utils.injector.
                require([Utils.viewModelPrefix + viewModelName], (viewModelCtr: new () => T) => {
                    var vm = new viewModelCtr();

                    activatorInstance.activateItem(vm).then((result: boolean) => {
                        if (!result) {
                            reject(new Error('unable to activate item'));
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
