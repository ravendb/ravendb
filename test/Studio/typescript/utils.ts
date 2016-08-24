/// <reference path="../../../src/Raven.Studio/typings/tsd.d.ts" />

import Squire = require("Squire");
import jquery = require("jquery");
import activator = require("durandal/activator");
import extensions = require("src/Raven.Studio/typescript/common/extensions");
import commandBaseMock = require("mocks/commandBaseMock");
import system = require("durandal/system");
import composition = require("durandal/composition");
import binder = require("durandal/binder");
import oauthContext = require("src/Raven.Studio/typescript/common/oauthContext");

system.debug(true);

class Utils { 

    static injector = new Squire();

    static initTest() {
        Utils.initInjector();
    }

    static initInjector() {
        extensions.install();

        beforeEach((cb: Function) => {
            
            Utils.injector = new Squire();
            Utils.injector.mock('knockout', ko);
            Utils.injector.mock('commands/commandBase', commandBaseMock);
            Utils.injector.store(['common/oauthContext', 'common/bindingHelpers/aceEditorBindingHandler'])
                .require(["mocks", "common/oauthContext", "common/bindingHelpers/aceEditorBindingHandler"], (mocks: any, context: oauthContext, aceEditorBindingHandler: any) => {
                    var ctx: any = mocks.store["common/oauthContext"];
                    ctx.enterApiKeyTask = $.Deferred().resolve();

                    aceEditorBindingHandler.useWebWorkers = false;
                    cb();
                });
        });

        afterEach(() => {
            Utils.injector.remove();
        });
    }

    static requireViewmodel<T>(viewmodelName: string, cb: Function) {
        Utils.injector.require(["viewmodels/" + viewmodelName], cb);
    }

    static mockCommand<T>(commandName: string, resultProvider:() => T) {
        Utils.injector.mock(commandName, () => ({
            execute: () => $.Deferred<T>().resolve(resultProvider())
        }));
    }

    static viewModelPrefix = "src/Raven.Studio/typescript/viewmodels/";
    static viewTemplatePrefix = "src/Raven.Studio/wwwroot/App/views/";
    static viewTemplateSuffix = ".html";

    static cleanup(activatorInstance: DurandalActivator<any>, $test: JQuery) {
        activatorInstance(null);
        ko.cleanNode($test[0].children[0]);
        $test.html("");
    }

    static runViewmodelTest<T>(viewModelName: string, opts: {
        initViewmodel?: (vm: T) => void;
        afterAttach?: (vm: T) => void;
        afterComposition?: (vm: T) => void;
        assertions?: (vm: T, $container: JQuery) => void;
    }): Promise<void> {
        return new Promise<void>((resolve, reject) => {
            Utils.injector.
                require([Utils.viewModelPrefix + viewModelName], (viewModel: new () => T) => {
                    try {
                        var vm = new viewModel();

                        var activatorInstance = activator.create();
                        activatorInstance.activateItem(vm).then((result: boolean) => {
                            if (!result) {
                                reject('unable to activate item');
                                return;
                            }
                            if (opts.initViewmodel) {
                                opts.initViewmodel(vm);
                            }
                            var $test = jquery("#test");

                            binder.throwOnErrors = true;

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
                                compositionComplete: () => {
                                    if (opts.afterComposition) {
                                        opts.afterComposition(vm);
                                    }
                                    setTimeout(() => {
                                        if (opts.assertions) {
                                            opts.assertions(vm, $test);
                                        }

                                        Utils.cleanup(activatorInstance, $test);

                                        resolve();
                                    });
                                }
                            }, null);
                            
                        });
                    } catch (e) {
                        reject(e);
                    }
                });
        });
    }
}

export = Utils;
