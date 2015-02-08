/// <reference path="../../Scripts/typings/bootstrap.v3.datetimepicker/bootstrap.v3.datetimepicker.d.ts" />

import composition = require("durandal/composition");
import moment = require("moment");

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
        var dpicker = $(element).datetimepicker(options);

        dpicker.on('dp.change', function (ev) {
            if (options.endDateElement) {
                $("#" + options.endDateElement).data("DateTimePicker").setMinDate(ev.date);
            }
            if (options.startDateElement) {
                $("#" + options.startDateElement).data("DateTimePicker").setMaxDate(ev.date);
            }

            var newDate = moment(ev.date);
            var value = valueAccessor();
            value(newDate);
        });
    }

    // Called by Knockout each time the dependent observable value changes.
    update(element: HTMLElement, valueAccessor, allBindings, viewModel, bindingContext: any) {
        var date : Moment =  ko.unwrap(valueAccessor());
        if (date) {
            $(element).data("DateTimePicker").setDate(date);
        }
    }
}

export = datePickerBindingHandler;