/// <reference path="../../../src/Raven.Studio/typings/tsd.d.ts" />

import Squire = require("Squire");
import jquery = require("jquery");
import activator = require("durandal/activator");
import extensions = require("src/Raven.Studio/typescript/common/extensions");
import commandBaseMock = require("mocks/commandBaseMock");
import system = require("durandal/system");
import oauthContext = require("src/Raven.Studio/typescript/common/oauthContext");

system.debug(true);

class Utils {

    static initTest() {
        Utils.initInjector();
    }

    static initInjector() {
        extensions.install();

        beforeEach(function (cb) {
            injector = new Squire();
            injector.mock('commands/commandBase', commandBaseMock);
            injector.store('common/oauthContext')
                .require(["mocks", "common/oauthContext"], function (mocks: any, context: any) {

                    var ctx: any = mocks.store["common/oauthContext"];
                    ctx.enterApiKeyTask = $.Deferred().resolve();
                    cb();
                });
        });

        afterEach(function () {
            injector.remove();
        });
    }

    static mockCommand<T>(commandName: string, resultValue: T) {
        injector.mock(commandName, () => ({
            execute: () => $.Deferred<T>().resolve(resultValue)
        }));
    }

    static viewModelPrefix = "src/Raven.Studio/typescript/viewmodels/";
    static viewTemplatePrefix = "text!src/Raven.Studio/wwwroot/App/views/";
    static viewTemplateSuffix = ".html";

    static runViewmodelTest<T>(viewModelName: string, opts: {
        initViewmodel?: (vm: T) => void,
        afterAttach?: (vm: T) => void,
        afterBinding?: (vm: T) => void
    }): Promise<void> {
        return new Promise<void>((resolve, reject) => {
            injector.
                require([Utils.viewModelPrefix + viewModelName, Utils.viewTemplatePrefix + viewModelName + Utils.viewTemplateSuffix], (viewModel: new () => T, viewTemplate: string) => {
                    try {
                        var vm = new viewModel();

                        var activatorInstance = activator.create();

                        activatorInstance(vm);
                        if (!activatorInstance()) {
                            reject('Unable to activate: ' + viewModelName);
                        }

                        if (opts.initViewmodel) {
                            opts.initViewmodel(vm);
                        }

                        if ((<any>vm).binding) {
                            (<any>vm).binding();
                        }

                        var $container = jquery("<div></div>");
                        var $test = jquery("#test");
                        var testNode = $test[0];

                        $container.html(viewTemplate);

                        ko.applyBindings(vm, $container[0]);

                        if (opts.afterBinding) {
                            opts.afterBinding(vm);
                        }

                        if ((<any>vm).bindingComplete) {
                            (<any>vm).bindingComplete();
                        }

                        $test.append($container);

                        if ((<any>vm).attached) {
                            (<any>vm).attached();
                        }

                        if ((<any>vm).compositionComplete) {
                            (<any>vm).compositionComplete();
                        }

                        if (opts.afterAttach) {
                            opts.afterAttach(vm);
                        }

                        activatorInstance(null);

                        ko.cleanNode(testNode);
                        $test.html('');

                        resolve();
                    } catch (e) {
                        reject(e);
                    }
                });
        });
    }
}

export = Utils;
