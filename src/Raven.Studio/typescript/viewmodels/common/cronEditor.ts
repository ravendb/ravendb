/// <reference path="../../../typings/tsd.d.ts" />

import getCronExpressionOccurrenceCommand = require("commands/database/tasks/getCronExpressionOccurrenceCommand");
import generalUtils = require("common/generalUtils");
import popoverUtils = require("common/popoverUtils");

type cronPeriod = "minute" | "hour" | "day" | "week" | "month" | "year" | "custom";

class cronEditor {
    targetField: KnockoutObservable<string>;
    
    static periods = ["minute", "hour", "day", "week", "month", "year", "custom"] as cronPeriod[];
    
    static hours = [] as valueAndLabelItem<string, string>[];
    static minutes = [] as number[];
    static daysOfWeek = [] as valueAndLabelItem<string, string>[];
    static daysOfMonth = [] as number[];
    static months = [] as valueAndLabelItem<string, string>[];
    
    period = ko.observable<cronPeriod>("minute");
    minutes = ko.observable<string>("0");
    hour = ko.observable<string>("0");
    dayOfWeek = ko.observable<string>("0");
    dayOfMonth = ko.observable<string>("1");
    month = ko.observable<string>("1");
    
    minutesVisible: KnockoutComputed<boolean>;
    timeVisible: KnockoutComputed<boolean>;
    dayOfWeekVisible: KnockoutComputed<boolean>;
    dayOfMonthVisible: KnockoutComputed<boolean>;
    monthVisible: KnockoutComputed<boolean>;

    humanReadableVisible: KnockoutComputed<boolean>;
    textInputVisible: KnockoutComputed<boolean>;
    
    monthLabel = ko.pureComputed(() => cronEditor.months.find(x => x.value === this.month()).label);
    dayOfWeekLabel = ko.pureComputed(() => cronEditor.daysOfWeek.find(x => x.value === this.dayOfWeek()).label);
    hourLabel = ko.pureComputed(() => cronEditor.hours.find(x => x.value === this.hour()).label);
    
    humanReadable: KnockoutComputed<string>;
    parsingError = ko.observable<string>();
    nextOccurrenceServerTime = ko.observable<string>("N/A");
    nextOccurrenceLocalTime = ko.observable<string>();
    nextInterval = ko.observable<string>();
    canDisplayNextOccurrenceLocalTime: KnockoutComputed<boolean>;
    
    constructor(targetField: KnockoutObservable<string>) {
        this.targetField = targetField;
        
        this.populateForm();
        
        this.initObservables();
    }
    
    attached(view: HTMLElement) {
        this.initTooltip(view);
    }
    
    private populateForm() {
        const value = this.targetField();
     
        if (value) {
            const parts = value.split(" ");
            if (parts.length !== 5) {
                this.period("custom");
            } else {
                if (/^(\*\s){4}\*$/.test(value)) { // "* * * * *"
                    this.period("minute");
                } else if (/^\d{1,2}\s(\*\s){3}\*$/.test(value)) { // "? * * * *"
                    this.period("hour");
                    this.minutes(parts[0]);
                } else if (/^(\d{1,2}\s){2}(\*\s){2}\*$/.test(value)) {// "? ? * * *"
                    this.period("day");
                    this.minutes(parts[0]);
                    this.hour(parts[1]);
                } else if (/^(\d{1,2}\s){2}(\*\s){2}\d{1,2}$/.test(value)) { // "? ? * * ?"
                    this.period("week");
                    this.minutes(parts[0]);
                    this.hour(parts[1]);
                    this.dayOfWeek(parts[4]);
                } else if (/^(\d{1,2}\s){3}\*\s\*$/.test(value)) { // "? ? ? * *"
                    this.period("month");
                    this.minutes(parts[0]);
                    this.hour(parts[1]);
                    this.dayOfMonth(parts[2]);
                } else if (/^(\d{1,2}\s){4}\*$/.test(value)) { // "? ? ? ? *"
                    this.period("year");
                    this.minutes(parts[0]);
                    this.hour(parts[1]);
                    this.dayOfMonth(parts[2]);
                    this.month(parts[3]);
                } else {
                    this.period("custom");
                }
            }
        } else {
            this.period("minute");
        }
    }
    
    private initObservables() {
        this.minutesVisible = ko.pureComputed(() => {
            const period = this.period();
            return period === "hour";
        });

        this.timeVisible = ko.pureComputed(() => {
            const period = this.period();
            return period === "day" || period === "week" || period === "month" || period === "year";
        });

        this.dayOfWeekVisible = ko.pureComputed(() => {
            const period = this.period();
            return period === "week";
        });

        this.dayOfMonthVisible = ko.pureComputed(() => {
            const period = this.period();
            return period === "month" || period === "year";
        });

        this.monthVisible = ko.pureComputed(() => {
            const period = this.period();
            return period === "year";
        });
        
        this.period.subscribe(() => this.update());
        this.minutes.subscribe(() => this.update());
        this.hour.subscribe(() => this.update());
        this.dayOfWeek.subscribe(() => this.update());
        this.dayOfMonth.subscribe(() => this.update());
        this.month.subscribe(() => this.update());

        this.humanReadable = ko.pureComputed(() => cronEditor.getHumanReadable(this.targetField(), this.parsingError));
        this.humanReadableVisible = ko.pureComputed(() => this.period() === "custom");
        this.textInputVisible = ko.pureComputed(() => this.period() === "custom");
        
        this.targetField.throttle(500).subscribe(() => this.updateNextOccurence());
        
        if (this.targetField()) {
            this.updateNextOccurence();
        }

        this.canDisplayNextOccurrenceLocalTime = ko.pureComputed(() => this.nextOccurrenceLocalTime() !== this.nextOccurrenceServerTime());
        
        this.targetField.extend({
            validation: [
                {
                    validator: () => !this.parsingError(),
                    message: `{0}`,
                    params: this.parsingError
                }
            ]
        })
    }
    
    initTooltip(container: HTMLElement) {
        const $container = $(container);
        popoverUtils.longWithHover($(".js-schedule-info", $container),
            {
                content:
                    "<div class='schedule-info-text'>" +
                    "Backup schedule is defined by a cron expression that can represent fixed times, dates, or intervals.<br/>" +
                    "We support cron expressions which consist of 5 <span style='color: #B9F4B7'>Fields</span>.<br/>" +
                    "Each field can contain any of the following <span style='color: #f9d291'>Values</span> along with " +
                    "various combinations of <span style='color: white'>Special Characters</span> for that field.<br/>" +
                    "<pre>" +
                    "+----------------> minute (<span class='values'>0 - 59</span>) (<span class='special-characters'>, - * /</span>)<br/>" +
                    "|  +-------------> hour (<span class='values'>0 - 23</span>) (<span class='special-characters'>, - * /</span>)<br/>" +
                    "|  |  +----------> day of month (<span class='values'>1 - 31</span>) (<span class='special-characters'>, - * ? / L W</span>)<br/>" +
                    "|  |  |  +-------> month (<span class='values'>1-12 or JAN-DEC</span>) (<span class='special-characters'>, - * /</span>)<br/>" +
                    "|  |  |  |  +----> day of week (<span class='values'>0-6 or SUN-SAT</span>) (<span class='special-characters'>, - * ? / L #</span>)<br/>" +
                    "|  |  |  |  |<br/>" +
                    "<span style='color: #B9F4B7'>" +
                    "<small><i class='icon-star-filled'></i></small>&nbsp;" +
                    "<small><i class='icon-star-filled'></i></small>&nbsp;" +
                    "<small><i class='icon-star-filled'></i></small>&nbsp;" +
                    "<small><i class='icon-star-filled'></i></small>&nbsp;" +
                    "<small><i class='icon-star-filled'></i></small>" +
                    "</span></pre><br/>" +
                    "For more information see: <a href='https://ravendb.net/l/X6IBEZ' target='_blank'>CronTrigger Tutorial</a></div>"
            });
    }
    
    private updateNextOccurence() {
        const cronExpression = this.targetField();

        if (this.parsingError() || !cronExpression) {
            this.nextOccurrenceServerTime("N/A");
            this.nextOccurrenceLocalTime("");
            this.nextInterval("");
            return;
        }

        const dateFormat = generalUtils.dateFormat;
        new getCronExpressionOccurrenceCommand(cronExpression)
            .execute()
            .done((result: Raven.Server.Web.Studio.StudioTasksHandler.NextCronExpressionOccurrence) => {
                if (result.IsValid) {
                    this.nextOccurrenceServerTime(moment(result.ServerTime).format(dateFormat));
                    const nextOccurrenceUtc = moment.utc(result.Utc);
                    this.nextOccurrenceLocalTime(nextOccurrenceUtc.local().format(dateFormat));

                    const fromDuration = generalUtils.formatDurationByDate(nextOccurrenceUtc, true);
                    this.nextInterval(`${fromDuration}`);

                    this.parsingError(null);
                } else {
                    this.nextOccurrenceServerTime("N/A");
                    this.nextOccurrenceLocalTime("");
                    this.nextInterval("");
                    this.parsingError(result.ErrorMessage);
                }
            });
    }

    private static getHumanReadable(frequency: string,
                                    parsingError: KnockoutObservable<string>): string {
        if (!frequency) {
            parsingError(null);
            return "N/A";
        }

        const frequencySplit = frequency.trim().replace(/ +(?= )/g, "").split(" ");
        if (frequencySplit.length < 5) {
            parsingError(`Expression has only ${frequencySplit.length} part` +
                `${frequencySplit.length === 1 ? "" : "s"}. ` +
                "Exactly 5 parts are required!");
            return "N/A";
        } else if (frequencySplit.length > 5) {
            parsingError(`Expression has ${frequencySplit.length} parts.` +
                "Exactly 5 parts are required!");
            return "N/A";
        }

        try {
            const result = cronstrue.toString(frequency.toUpperCase());
            if (result.includes("undefined")) {
                parsingError("Invalid cron expression!");
                return "N/A";
            }

            parsingError(null);
            return result;
        } catch (error) {
            parsingError(error);
            return "N/A";
        }
    }
    
    private update() {
        this.targetField(this.getCronExpression());
    }
    
    private getCronExpression() {
        switch (this.period()) {
            case "minute":
                return "* * * * *";
            case "hour":
                return this.minutes() + " * * * *";
            case "day":
                return this.minutes() + " " + this.hour() + " * * *";
            case "week":
                return this.minutes() + " " + this.hour() + " * * " + this.dayOfWeek();
            case "month":
                return this.minutes() + " " + this.hour() + " " + this.dayOfMonth() + " * *";
            case "year":
                return this.minutes() + " " + this.hour() + " " + this.dayOfMonth() + " " + this.month() + " *";
            case "custom":
                return this.targetField();
        }
    }

}

for (let i = 0; i < 60; i++) {
    cronEditor.minutes.push(i);
}

for (let i = 0; i < 24; i++) {
    const hour = i % 12;
    
    cronEditor.hours.push({
        label: (hour || "12") + " " + (i < 12 ? "AM":"PM"),
        value: i.toString()
    });
}

for (let i = 1; i <= 31; i++) {
    cronEditor.daysOfMonth.push(i);
}

["January", "February", "March", "April",
    "May", "June", "July", "August",
    "September", "October", "November", "December"].forEach((month, idx) => {
    cronEditor.months.push({
        label: month,
        value: (idx + 1).toString()
    });  
});

["Sunday", "Monday", "Tuesday", "Wednesday", "Thursday", "Friday", "Saturday"].forEach((dayOfWeek, idx) => {
    cronEditor.daysOfWeek.push({
        label: dayOfWeek,
        value: idx.toString()
    });
})

export = cronEditor;
