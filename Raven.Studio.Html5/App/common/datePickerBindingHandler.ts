/// <reference path="../../Scripts/typings/bootstrap.datepicker/bootstrap.datepicker.d.ts" />

import composition = require("durandal/composition");
import moment = require("moment");

/*
 * A custom Knockout binding handler transforms the target element (usually a <pre>) into a code editor, powered by Ace. http://ace.c9.io
 * Usage: data-bind="aceEditor: { code: someObservableString, lang: 'ace/mode/csharp', theme: 'ace/theme/github', fontSize: '16px' }"
 * All params are optional, except code.
 */
class datePickerBindingHandler {

    static install() {
        if (!ko.bindingHandlers["datePicker"]) {
            ko.bindingHandlers["datePicker"] = new datePickerBindingHandler();

            // This tells Durandal to fire this binding handler only after composition 
            // is complete and attached to the DOM.
            // See http://durandaljs.com/documentation/Interacting-with-the-DOM/
            composition.addBindingHandler("datePicker");
        }
    }

    // Called by Knockout a single time when the binding handler is setup.
    init(element: HTMLElement, valueAccessor, allBindings, viewModel, bindingContext: any) {
        var options = allBindings().datepickerOptions || {};
        var dpicker = $(element).datepicker(options).on('changeDate',function(ev){
            var newDate = moment(ev.date);
            var value = valueAccessor();
            var currentDate = moment(value() || new Date);
            newDate.hours(currentDate.hours());
            newDate.minutes(currentDate.minutes());
            newDate.seconds(currentDate.seconds());
            value(newDate);
        });
    }

    // Called by Knockout each time the dependent observable value changes.
    update(element: HTMLElement, valueAccessor, allBindings, viewModel, bindingContext: any) {
        var date : Moment =  ko.unwrap(valueAccessor());
        if(date){
            $(element).datepicker('setDate', date.toDate());
        }
    }
}

export = datePickerBindingHandler;