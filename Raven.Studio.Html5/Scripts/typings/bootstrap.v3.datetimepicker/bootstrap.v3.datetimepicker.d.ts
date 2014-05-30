// Type definitions for Bootstrap datetimepicker v3 
// Project: http://eonasdan.github.io/bootstrap-datetimepicker
// Definitions by: Jesica N. Fera <https://github.com/bayitajesi>
// Definitions: https://github.com/borisyankov/DefinitelyTyped

/**
 * bootstrap-datetimepicker.js 3.0.0 Copyright (c) 2014 Jonathan Peterson
 * Available via the MIT license.
 * see: http://eonasdan.github.io/bootstrap-datetimepicker or https://github.com/Eonasdan/bootstrap-datetimepicker for details.
 */

/// <reference path="../jquery/jquery.d.ts"/>
/// <reference path="../moment/moment.d.ts"/>


interface DatetimepickerChangeEventObject extends JQueryEventObject {
    date: Moment;
    oldDate: Moment;
}

interface DatetimepickerEventObject extends JQueryEventObject {
    date: Moment;
}

interface DatetimepickerIcons {
    time?: string;
    date?: string;
    up?: string;
    down?: string;
}

interface DatetimepickerOptions {
    pickDate?: boolean;
    pickTime?: boolean;
    useMinutes?: boolean;
    useSeconds?: boolean;
    useCurrent?: boolean;
    minuteStepping?: number;
    minDate?: any;
    maxDate?: any;
    showToday?: boolean;
    collapse?: boolean;
    language?: string;
    defaultDate?: string;
    disabledDates?: Array<any>;
    enabledDates?: Array<any>;
    icons?: DatetimepickerIcons;
    useStrict?: boolean;
    direction?: string;
    sideBySide?: boolean;
    daysOfWeekDisabled?: Array<any>;
}

interface Datetimepicker {
    setDate(date: any);
    setMinDate(date: any);
    setMaxDate(date: any);
    show();
    disable();
    enable();
    getDate();
}

interface JQuery {

    datetimepicker(): JQuery;
    datetimepicker(options: DatetimepickerOptions): JQuery;

    off(events: "dp.change", selector?: string, handler?: (eventobject: DatetimepickerChangeEventObject) => any): JQuery;
    off(events: "dp.change", handler: (eventobject: DatetimepickerChangeEventObject) => any): JQuery;

    on(events: "dp.change", selector: string, data: any, handler?: (eventobject: DatetimepickerChangeEventObject) => any): JQuery;
    on(events: "dp.change", selector: string, handler: (eventobject: DatetimepickerChangeEventObject) => any): JQuery;
    on(events: 'dp.change', handler: (eventObject: DatetimepickerChangeEventObject) => any): JQuery;

    off(events: "dp.show", selector?: string, handler?: (eventobject: DatetimepickerEventObject) => any): JQuery;
    off(events: "dp.show", handler: (eventobject: DatetimepickerEventObject) => any): JQuery;

    on(events: "dp.show", selector: string, data: any, handler?: (eventobject: DatetimepickerEventObject) => any): JQuery;
    on(events: "dp.show", selector: string, handler: (eventobject: DatetimepickerEventObject) => any): JQuery;
    on(events: 'dp.show', handler: (eventObject: DatetimepickerEventObject) => any): JQuery;

    off(events: "dp.hide", selector?: string, handler?: (eventobject: DatetimepickerEventObject) => any): JQuery;
    off(events: "dp.hide", handler: (eventobject: DatetimepickerEventObject) => any): JQuery;

    on(events: "dp.hide", selector: string, data: any, handler?: (eventobject: DatetimepickerEventObject) => any): JQuery;
    on(events: "dp.hide", selector: string, handler: (eventobject: DatetimepickerEventObject) => any): JQuery;
    on(events: 'dp.hide', handler: (eventObject: DatetimepickerEventObject) => any): JQuery;

    off(events: "dp.error", selector?: string, handler?: (eventobject: DatetimepickerEventObject) => any): JQuery;
    off(events: "dp.error", handler: (eventobject: DatetimepickerEventObject) => any): JQuery;

    on(events: "dp.error", selector: string, data: any, handler?: (eventobject: DatetimepickerEventObject) => any): JQuery;
    on(events: "dp.error", selector: string, handler: (eventobject: DatetimepickerEventObject) => any): JQuery;
    on(events: 'dp.error', handler: (eventObject: DatetimepickerEventObject) => any): JQuery;

    data(key: 'DateTimePicker'): Datetimepicker;
}