/// <reference path="../jquery/jquery.d.ts"/>
/// <reference path="../moment/moment.d.ts"/>


interface DatetimepickerEventObject extends JQueryEventObject {
    date: Moment;
    //format(format?: string): string;
}

interface DatetimepickerOptions {
}

interface Datetimepicker {
    setDate(date: Moment);
    setMinDate(date: Moment);
    setMaxDate(date: Moment);
}

interface JQuery {
    datetimepicker(): JQuery;
    datetimepicker(options: DatetimepickerOptions): JQuery;

    off(events: "dp.change", selector?: string, handler?: (eventobject: DatetimepickerEventObject) => any): JQuery;
    off(events: "dp.change", handler: (eventobject: DatetimepickerEventObject) => any): JQuery;

    on(events: "dp.change", selector: string, data: any, handler?: (eventobject: DatetimepickerEventObject) => any): JQuery;
    on(events: "dp.change", selector: string, handler: (eventobject: DatetimepickerEventObject) => any): JQuery;
    on(events: 'dp.change', handler: (eventObject: DatetimepickerEventObject) => any): JQuery;

    data(key: 'DateTimePicker'): Datetimepicker;
}