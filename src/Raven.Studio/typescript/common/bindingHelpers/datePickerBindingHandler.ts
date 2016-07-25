/// <reference path="../../../typings/tsd.d.ts" />

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
    init(element: HTMLElement, valueAccessor: any, allBindings: KnockoutAllBindingsAccessor, viewModel: any, bindingContext: any) {
        var options = allBindings().datepickerOptions || {};
        var endDateElement = options.endDateElement;
        var startDateElement = options.startDateElement;
        delete options.endDateElement;
        delete options.startDateElement;
        var dpicker = $(element).datetimepicker(options);

        dpicker.on('dp.change', ev => {
            if (endDateElement) {
                $("#" + endDateElement).data("DateTimePicker").minDate(ev.date);
            }
            if (startDateElement) {
                $("#" + startDateElement).data("DateTimePicker").maxDate(ev.date);
            }

            var newDate = moment(ev.date);
            var value = valueAccessor();
            value(newDate);
        });
    }

    // Called by Knockout each time the dependent observable value changes.
    update(element: HTMLElement, valueAccessor: any, allBindings: KnockoutAllBindingsAccessor, viewModel: any, bindingContext: any) {
        var date : moment.Moment =  ko.unwrap(valueAccessor());
        if (date) {
            $(element).data("DateTimePicker").date(date);
        }
    }
}

export = datePickerBindingHandler;
