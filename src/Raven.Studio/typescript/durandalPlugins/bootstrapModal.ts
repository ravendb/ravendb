/// <reference path="../../typings/tsd.d.ts" />

/*
Taken from: https://github.com/mbejda/BootstrapModalDurandalJS/blob/master/bootstrapModal.js

with LOCAL modifications!
*/

import system = require("durandal/system");
import dialog = require("plugins/dialog");
import app = require("durandal/app");
import viewEngine = require("durandal/viewEngine");
import ko = require("knockout");
import composition = require("durandal/composition");

let closeCalled = false;

dialog.addContext('bootstrapModal', {
        closeCalled: false,
        blockoutOpacity: .2,
        removeDelay: 300,
        addHost: (theDialog: dialog.Dialog) => {
            const body = $("body");
            
            let modalContainer = body;
            if (body.hasClass("fullscreen")) {
                // looks like we are in fullscreen mode - find modal root
                modalContainer = $(".modal-root");
                
                if (modalContainer.length === 0) {
                    throw new Error('Unable to find element with class .modal-root');
                }
            }
            
            const host = $('<div class="modal" id="bootstrapModal" tabindex="-1" role="dialog" aria-labelledby="bootstrapModal" aria-hidden="true"></div>')
                .appendTo(modalContainer);
            (theDialog as any).host = host.get(0);
            closeCalled = false;
        },
        removeHost: (theDialog: dialog.Dialog) => {
            closeCalled = true;
            $('#bootstrapModal').modal('hide');
            $('body').removeClass('modal-open');
            $('.modal-root').removeClass('modal-open');
        },
        attached: null,
        compositionComplete: (child: HTMLElement, parent: HTMLElement, context: composition.CompositionContext) => {
            var theDialog = dialog.getDialog(context.model);
            var options: ModalOptions = {};
            options.show = true;

            if ($(child).hasClass('prevent-close')) {
                options.backdrop = 'static';
                options.keyboard = false;
            }

            $('#bootstrapModal').modal(options);
            
            if ($("body").hasClass("fullscreen")) {
                $('.modal-backdrop').appendTo('.modal-root');   
            }
            
            $('#bootstrapModal').on('hidden.bs.modal', (e) => {
                if (!closeCalled) {
                    theDialog.close();
                }
                ko.removeNode(theDialog.host);
                $('.modal-backdrop').remove();
            });
        }
    } as any);

var bootstrapMarkup = [
    '<div data-view="plugins/messageBox" data-bind="css: getClass(), style: getStyle()">',
    '<div class="modal-content">',
    '<div class="modal-header">',
    '<h3 data-bind="html: title"></h3>',
    '</div>',
    '<div class="modal-body">',
    '<p class="message" data-bind="html: message"></p>',
    '</div>',
    '<div class="modal-footer">',
    '<!-- ko foreach: options -->',
    '<button data-bind="click: function () { $parent.selectOption($parent.getButtonValue($data)); }, text: $parent.getButtonText($data), css: $parent.getButtonClass($index)"></button>',
    '<!-- /ko -->',
    '<div style="clear:both;"></div>',
    '</div>',
    '</div>',
    '</div>'
].join('\n');

class bootstrapModal {
    static install() {

        app.showBootstrapDialog = (obj: any, activationData: any) => dialog.show(obj, activationData, 'bootstrapModal');

        app.showBootstrapMessage = (message: string, title: string, options: string[], autoclose: boolean, settings: any) => {
            return (dialog as any).showBootstrapMessage(message, title, options, autoclose, settings);
        };

        (dialog as any).showBootstrapDialog = (obj: any, activationData: any) => dialog.show(obj, activationData, 'bootstrapModal');

        (dialog as any).showBootstrapMessage = (message: string, title: string, options: any, autoclose: boolean, settings: any) => {
            if (system.isString(dialog.MessageBox)) {
                return dialog.show(dialog.MessageBox, [
                    message,
                    title || (dialog.MessageBox as any).defaultTitle,
                    options || (dialog.MessageBox as any).defaultOptions,
                    autoclose || false,
                    settings || {}
                ], 'bootstrapModal');
            }
            var bootstrapDefaults = {
                buttonClass: "btn btn-default",
                primaryButtonClass: "btn-primary autofocus",
                secondaryButtonClass: "",
                "class": "modal-dialog",
                style: null as any
            };

            (dialog.MessageBox as any).prototype.getView = () => viewEngine.processMarkup(bootstrapMarkup);

            var bootstrapSettings = $.extend(bootstrapDefaults, settings);
            return dialog.show(new (dialog.MessageBox as any)(message, title, options, autoclose, bootstrapSettings), {}, 'bootstrapModal');
        };

        (dialog.MessageBox as any).prototype.compositionComplete = (child: HTMLElement, parent: HTMLElement, context: composition.CompositionContext) => {
            var theDialog = dialog.getDialog(context.model);
            var $child = $(child);
            if ($child.hasClass('autoclose') || context.model.autoclose) {
                $((theDialog as any).blockout).click(() => {
                    theDialog.close();
                });
            }
        };
    }
}


export = bootstrapModal;
