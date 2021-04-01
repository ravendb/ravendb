/// <reference path="../../typings/tsd.d.ts" />
import pluralizeHelpers = require("common/helpers/text/pluralizeHelpers");
import timeHelpers = require("common/timeHelpers");
import moment = require("moment");
import d3 = require("d3");

class genUtils {
    
    static integerMaxValue = 2147483647;

    static entityMap: any = {
        '&': '&amp;',
        '<': '&lt;',
        '>': '&gt;',
        '"': '&quot;',
        "'": '&#39;',
        '/': '&#x2F;',
        '`': '&#x60;',
        '=': '&#x3D;'
    };

    static dateFormat = "YYYY MMMM Do, h:mm A";
    
    static readonly utcFullDateFormat = "YYYY-MM-DD[T]HH:mm:ss.SSS[Z]";
    
    /***  IP Address Methods  ***/

    static isLocalhostIpAddress(ip: string): boolean {
        return ((ip === 'localhost') || (_.split(ip, '.')[0] === '127') || (ip === '::1'));
    }

    static regexIPv4 = /^(?:(?:25[0-5]|2[0-4][0-9]|[01]?[0-9][0-9]?)\.){3}(?:25[0-5]|2[0-4][0-9]|[01]?[0-9][0-9]?)$/;
    
    static readonly urlRegex = /^(https?:\/\/)([^\s]+)$/; // allow any char, exclude white space
    static readonly invalidUrlMessage = "Url format expected: 'http(s)://hostName'";
    
    static isValidUrl(url: string) {
        let valid = true;
        
        try {
            new URL(url);
        } catch (e) {
            valid = false;
        }
        
        return valid;
    }
    
    static isHostname(address: string): boolean {
        const info = genUtils.getAddressInfo(address);
        return info.Type === "hostname";
    }

    static getAddressInfo(address: string): { Type: addressType, HasPort: boolean } {
        if (!address) {
            return {
                Type: "invalid",
                HasPort: undefined
            }
        }

        const maybeIpv6 = Array.from(address).filter(x => x === ":").length >= 2;

        try {
            if (maybeIpv6) {
                // it seems to be ipv6 address - try to parse this as http://[address]
                address = `[${address}]`;
            }
            
            const url = new URL("http://" + address);

            if (!maybeIpv6 && url.host !== address) {
                // this can happen! Try to call: new URL("http://127.0.1255:22") -> it returns: http://127.0.4.231:22/
                return {
                    Type: "invalid",
                    HasPort: undefined
                }
            }
            
            const hasPort = url.port !== "";

            const hostname = url.hostname;

            if (genUtils.regexIPv4.test(hostname)) {
                return {
                    Type: "ipv4",
                    HasPort: hasPort
                }
            }

            // at this point we know we have valid url and it isn't ipv4 (so it is hostname or ipv6)

            return {
                Type: hostname.includes(":") ? "ipv6" : "hostname",
                HasPort: hasPort
            }
        } catch (e) {
            return {
                Type: "invalid",
                HasPort: undefined
            }
        }
    }
    
    /***  Date-Time Methods  ***/

    static timeSpanAsAgo(input: string, withSuffix: boolean): string {
        if (!input) {
            return null;
        }

        return moment.duration("-" + input).humanize(withSuffix);
    }

    static formatDuration(duration: moment.Duration, longFormat = false, desiredAccuracy = 5, skipSecondsAndMilliseconds = false) {
        const timeTokens = [] as Array<string>;

        if (duration.years() >= 1) {
            timeTokens.push(longFormat ?
                pluralizeHelpers.pluralize(Math.floor(duration.years()), "year ", "years ") :
                Math.floor(duration.years()) + " y ");
        }
        if (duration.months() >= 1) {
            timeTokens.push(longFormat ?
                pluralizeHelpers.pluralize(Math.floor(duration.months()), "month ", "months ") :
                Math.floor(duration.months()) + " m ");
        }
        if (duration.days() >= 1) {
            timeTokens.push(longFormat ?
                pluralizeHelpers.pluralize(Math.floor(duration.days()), "day ", "days ") :
                Math.floor(duration.days()) + " d ");
        }
        if (duration.hours() > 0) {
            timeTokens.push(longFormat ?
                pluralizeHelpers.pluralize(duration.hours(), "hour ", "hours ") :
                duration.hours() + " h ");
        }
        if (duration.minutes() > 0) {
            timeTokens.push(longFormat ?
                pluralizeHelpers.pluralize(duration.minutes(), "minute ", "minutes ") :
                duration.minutes() + " m ");
        }
        if (duration.seconds() > 0 && !skipSecondsAndMilliseconds) {
            timeTokens.push(longFormat ?
                pluralizeHelpers.pluralize(duration.seconds(), "second ", "seconds ") :
                duration.seconds() + " s ");
        }
        if (duration.milliseconds() > 0 && !skipSecondsAndMilliseconds) {
            const millis = duration.milliseconds();

            const atLeastOneSecond = duration.asSeconds() >= 1;
            timeTokens.push((atLeastOneSecond ? Math.floor(millis) : Math.floor(millis * 100) / 100) + " ms ");
        }
        
        if (timeTokens.length > desiredAccuracy) {
            timeTokens.length = desiredAccuracy;
        }

        if (timeTokens.length === 0) {
            if (skipSecondsAndMilliseconds) {
                timeTokens.push("less than a minute");
            } else {
                timeTokens.push("0 ms");
            }
        }

        return timeTokens.join(" ");
    }
    
    static formatUtcDateAsLocal(date: string, format: string = genUtils.dateFormat) {
        const dateToFormat = moment.utc(date);
        return dateToFormat.local().format(format);
    }

    static formatDurationByDate(dateInUtc: moment.Moment, addTimeText: boolean = false): string {
        const timeDiff = moment.utc().diff(dateInUtc);
        
        const futureTime = timeDiff < 0; 
        const diff = futureTime ? timeDiff * -1 : timeDiff;
        
        const duration = genUtils.formatDuration(moment.duration(diff), true, 2, true);
        
        if (!addTimeText) {
            return duration; 
        }
        
        return `${futureTime ? "in" : ""} ${duration} ${!futureTime ? "ago" : ""}`;
    }

    static formatMillis(input: number) {
        return genUtils.formatDuration(moment.duration({
            milliseconds: input
        }));
    }

    static readonly timeSpanMaxValue = "10675199.02:48:05.4775807";
    
    static formatTimeSpan(input: string | number, longFormat = false) {
        return genUtils.formatDuration(moment.duration(input), longFormat);
    }

    static timeSpanToSeconds(input: string) {
        if (!input) {
            return null;
        }
        return moment.duration(input).asSeconds();
    }

    static formatAsTimeSpan(millis: number) {
        const duration = moment.duration({
            milliseconds: millis
        });

        const formatNumber = (input: number) => _.padStart(input.toString(), 2, '0');

        const days = Math.floor(duration.asDays());
        if (days) {
            return `${days}.${formatNumber(duration.hours())}:${formatNumber(duration.minutes())}:${formatNumber(duration.seconds())}`;
        }

        return `${formatNumber(Math.floor(duration.asHours()))}:${formatNumber(duration.minutes())}:${formatNumber(duration.seconds())}`;
    }

    static toHumanizedDate(input: string) {
        const dateMoment = moment(input.toString());
        if (dateMoment.isValid()) {
            const now = moment();
            const agoInMs = dateMoment.diff(now);
            return moment.duration(agoInMs).humanize(true) + dateMoment.format(" (MM/DD/YY, h:mma)");
        }

        return input;
    }

    /***  Size Methods ***/

    // Format bytes to human size string
    static formatBytesToSize(bytes: number, digitsAfterDecimalPoint = 2): string {
        const sizes = ['Bytes', 'KB', 'MB', 'GB', 'TB'];
        if (bytes === 0) {
            return "0 Bytes";
        }
        if (bytes === 1) {
            return "1 Byte";
        }
        if (!bytes || bytes === -1) return 'n/a';
        const i = Math.floor(Math.log(bytes) / Math.log(1024));

        if (i < 0) {
            // number < 1
            return genUtils.formatAsCommaSeperatedString(bytes, digitsAfterDecimalPoint) + ' Bytes';
        }

        const res = bytes / Math.pow(1024, i);
        const newRes = genUtils.formatAsCommaSeperatedString(res, digitsAfterDecimalPoint);

        return newRes + ' ' + sizes[i];
    }

    static getSizeInBytesAsUTF8(input: string) {
        let result = 0;
        let isQuoted = false;
        let prevChar: any = 0;
        for (let n = 0; n < input.length; n++) {

            const charCode = genUtils.fixedCharCodeAt(input, n);

            if (charCode === 34 /*quates*/) {
                if (!(isQuoted && prevChar === 92 /*backslash*/)) {
                    isQuoted = !isQuoted;
                }
            }

            prevChar = charCode;

            // whiteSpaceCharacters list from : https://en.wikipedia.org/wiki/Whitespace_character
            const whiteSpaceCharacters = [9, 10, 11, 12, 13, 32, 133, 160, 5760, 8192, 8193, 8194, 8195, 8196, 8197, 8198, 8199, 8200, 8201, 8202, 8232, 8233, 8239, 8287, 12288, 6158, 8203, 8204, 8205, 8288, 65279];
            if (isQuoted === false && $.inArray(charCode, whiteSpaceCharacters) > -1) {
                continue;
            }

            if (typeof charCode === "number") {
                if (charCode < 128) {
                    result = result + 1;
                } else if (charCode < 2048) {
                    result = result + 2;
                } else if (charCode < 65536) {
                    result = result + 3;
                } else if (charCode < 2097152) {
                    result = result + 4;
                } else if (charCode < 67108864) {
                    result = result + 5;
                } else {
                    result = result + 6;
                }
            }
        }
        return result;
    };

    static getSizeClass(input: number): string {
        if (input < 100000) {
            return "";
        }
        if (input < 1000 * 1000) {
            return "kilo";
        }
        return "mega";
    }
    
    static siFormat(value: number) {
        if (value <= 999) {
            return value.toFixed(0);
        }
        const format = d3.formatPrefix(value);
        let scaledValue = format.scale(value).toFixed(1);
        if (scaledValue.endsWith(".0")) {
            scaledValue = scaledValue.substring(0, scaledValue.length - 2);
        }
        // trim zeros
        return scaledValue + format.symbol;
    }

    static getCountPrefix(count: number): string {
        if (count < 100000) {
            return count.toLocaleString();
        }
        if (count < 1000 * 1000) {
            return _.floor(count / 1000, 2).toLocaleString();
        }
        return _.floor(count / 1000000, 2).toLocaleString();
    }
    
    static getSelectedText() {
        if (window.getSelection) {
            return window.getSelection().toString();
        } else if ((document as any).selection) {
            return (document as any).selection.createRange().text;
        }
        return '';
    }
    
    /***  String Methods ***/

    static trimMessage(message: any, limit: number = 256) {
        if (!message) {
            return message;
        }
        if (typeof message !== "string") {
            message = message.toString();
        }
        
        const lineBreakIdx = Math.min(message.indexOf("\r"), message.indexOf("\r"));
        if (lineBreakIdx !== -1 && lineBreakIdx < 256) {
            return message.substr(0, lineBreakIdx);
        }

        if (message.length < limit) {
            return message;
        }

        return message.substr(0, limit) + "...";
    }

    static sortedAlphaNumericIndex<T>(items: T[], newItem: T, extractor: (item: T) => string): number {
        if (!items.length) {
            return 0;
        }
        
        const newItemValue = extractor(newItem);
        
        for (let i = 0; i < items.length; i++) {
            const currentValue = extractor(items[i]);
            const cmp = genUtils.sortAlphaNumeric(currentValue, newItemValue);
            if (cmp >= 0) {
                return i;
            } 
        }
        
        return items.length;
    }
    
    static sortAlphaNumeric(a: string, b: string, mode: sortMode = "asc"): number {
        const result = genUtils.sortAlphaNumericInternal(a, b);
        if (result === 0)
            return 0;

        return mode === "asc" ? result : -result;
    }

    private static sortAlphaNumericInternal(a: string, b: string): number {
        if (a === b) {
            return 0;
        }
        const aInt = parseInt(a, 10);
        const bInt = parseInt(b, 10);

        const aIsNan = isNaN(aInt);
        const bIsNan = isNaN(bInt);
        if (aIsNan && bIsNan) {
            const reA = /[0-9]+$/g;
            const aA = a.replace(reA, "");
            const bA = b.replace(reA, "");
            if (aA.toLowerCase() === bA.toLowerCase()) {
                const reN = /[^0-9]/g;
                const aN = parseInt(a.replace(reN, ""), 10);
                const bN = parseInt(b.replace(reN, ""), 10);
                return aN === bN ? 0 : aN > bN ? 1 : -1;
            }

            return a.toLowerCase() > b.toLowerCase() ? 1 : -1;

        } else if (aIsNan) { // a is not a number
            return 1;
        } else if (bIsNan) { // b is not a number
            return -1;
        }

        return aInt === bInt ? 0 : aInt > bInt ? 1 : -1;
    }
    
    static formatAsCommaSeperatedString(input: number, digitsAfterDecimalPoint: number) {
        const parts = input.toString().split(".");
        parts[0] = parts[0].replace(/\B(?=(\d{3})+(?!\d))/g, ",");

        if (digitsAfterDecimalPoint > 0) {
            if (parts.length == 2 && parts[1].length > digitsAfterDecimalPoint) {
                parts[1] = parts[1].substring(0, digitsAfterDecimalPoint);
            }
            return parts.join(".");
        }
        
        return parts[0];
    }

    // Replace characters with their char codes, but leave A-Za-z0-9 and - in place. 
    static escape(input: string) {
        let output = "";
        for (let i = 0; i < input.length; i++) {
            const ch = input.charCodeAt(i);
            if (ch == 0x2F) {
                output += '-';
            } else if (ch >= 0x30 && ch <= 0x39 || ch >= 0x41 && ch <= 0x5A || ch >= 0x61 && ch <= 0x7A || ch == 0x2D) {
                output += input[i];
            } else {
                output += ch;
            }
        }
        return output;
    }

    static nl2br(str: string) {
        return str = str.replace(/(?:\r\n|\r|\n)/g, '<br>');
    }

    static escapeHtml(string: string) {
        if (!string) {
            return string;
        }
        
        return String(string).replace(/[&<>"'`=\/]/g, s => genUtils.entityMap[s]);
    }
    
    static unescapeHtml(string: string) {
        if (!string) {
            return string;
        }
        return $("<div/>").html(string).text();
    }

    // Return the inputNumber as a string with separating commas rounded to 'n' decimal digits
    // (e.g. for n==2: 2046430.45756 => "2,046,430.46")
    static formatNumberToStringFixed(inputNumber: number, n: number): string {
        return inputNumber.toLocaleString(undefined, { minimumFractionDigits: n, maximumFractionDigits: n });
    };

    static getItemsListFormatted(items: Array<string>) {
        switch (items.length) {
            case 0:
                return "";
            case 1:
                return `${items[0]}`;
            case 2:
                return `${items[0]} & ${items[1]}`;
            default:
                return `${items.slice(0, items.length-1).join(', ')} & ${items[items.length-1]}`;
        }
    }
    
    static stringify(obj: any, stripNullAndEmptyValues: boolean = false) {
        const prettifySpacing = 4;

        if (stripNullAndEmptyValues) {
            return JSON.stringify(obj, (key, val) => {
                const isNull = _.isNull(val);
                const isEmptyObj = _.isEqual(val, {});
                const isEmptyArray = _.isEqual(val, []);

                return isNull || isEmptyObj || isEmptyArray ? undefined : val;

            }, prettifySpacing);
        } else {
            return JSON.stringify(obj, null, prettifySpacing);
        }
    }

    /***  File Methods ***/
    
    static getFileExtension(filePath: string): string {
        const fileParts = _.split(filePath, ".");
        
        return fileParts.length > 1 ? fileParts.pop() : null;
    }

    /***  Distance Methods ***/

    // input: Miles/KM, output: Meters 
    static getMeters(distance: number, units: Raven.Client.Documents.Indexes.Spatial.SpatialUnits) {
        return units === "Miles" ? distance * 1609.344 : distance * 1000;
    }
    
    /***  Other Methods ***/

    static delayedSpinner(spinner: KnockoutObservable<boolean>, promise: JQueryPromise<any>, delay = 100) {
        const spinnerTimeout = setTimeout(() => {
            spinner(true);
        }, delay);
        
        promise.always(() => {
            clearTimeout(spinnerTimeout);
            
            if (spinner()) {
                spinner(false);
            }
        })
    }
    
    static debounceAndFunnel<T>(func: (val: T, 
                                       params: any, 
                                       callback: (currentValue: T, errorMessageOrValidationResult: string | boolean) => void) => void) {
        
        return _.debounce((val: T, 
                           params: any,
                           internalCallback: (result: { isValid: boolean, message: string } | boolean) => void) => {
                                            func(val, params, (currentValue, result) => {
                                                   if (currentValue === val) {
                                                       if (_.isBoolean(result)) {
                                                           internalCallback(result);
                                                       } else if (result) {
                                                           internalCallback({ isValid: false, message: result});
                                                       } else {
                                                           internalCallback(true);
                                                       }
                                                   }
                                      });
                           }, 500);
    }

    static hashCode(input: string) {
        let hash = 0;
        if (input.length === 0) return hash;
        for (let i = 0; i < input.length; i++) {
            const char = input.charCodeAt(i);
            hash = ((hash << 5) - hash) + char;
            hash |= 0; // Convert to 32bit integer
        }
        return hash;
    }

    static escapeForShell(input: string) {
        return '"' + input.replace(/[\r\n]/g, "").replace(/(["\\])/g, '\\$1') + '"';
    }

    private static fixedCharCodeAt(input: string, idx: number) {
        idx = idx || 0;
        const code = input.charCodeAt(idx);
        let hi: number, low: number;
        if (0xD800 <= code && code <= 0xDBFF) { // High surrogate (could change last hex to 0xDB7F to treat high private surrogates as single characters)
            hi = code;
            low = input.charCodeAt(idx + 1);
            if (isNaN(low)) {
                throw 'No valid character or memory error!';
            }
            return ((hi - 0xD800) * 0x400) + (low - 0xDC00) + 0x10000;
        }
        if (0xDC00 <= code && code <= 0xDFFF) { // Low surrogate
            // We return false to allow loops to skip this iteration since should have already handled high surrogate above in the previous iteration
            return 0;
        }
        return code;
    };
    
    static findLongestLine(htmlText: string, lineSeparator: string = "<br/>") : string {
        // Find and return the longest line in an html text
        const textLines = htmlText.split(lineSeparator);
        return textLines.reduce((a, b) => (a.length > b.length) ? a : b, "");
    }

    static findNumberOfLines(htmlText: string, lineSeparator: string = "<br/>"): number {
        // Find and return the number of lines in an html text
        const textLines = htmlText.split(lineSeparator);
        return textLines.length;
    }
    
    static canConsumeDelegatedEvent(event: JQueryEventObject) {
        const target = event.target;
        const currentTarget = event.currentTarget;
        
        let element = target;
        
        while (element != currentTarget) {
            
            const tag = element.tagName.toLocaleLowerCase();
            
            if (tag === "a" || tag === "button" || tag === "input") {
                return false;
            }
            
            element = element.parentElement;
        }
        
        return true;
    }   
    
    static generateUUID() {
        let dt = new Date().getTime();
        const uuid = 'xxxxxxxx-xxxx-4xxx-yxxx-xxxxxxxxxxxx'.replace(/[xy]/g, function(c) {
            const r = (dt + Math.random()*16)%16 | 0;
            dt = Math.floor(dt/16);
            return (c=='x' ? r :(r&0x3|0x8)).toString(16);
        });
        return uuid;
    }
    
    static inMemoryRender(templateName: string, data: any) {
        const div = $("<div>");
        
        try {
            ko.applyBindingsToNode(div[0], { template: { name: templateName, data } });
        } catch (e) {
            console.error(e);
            return "error";
        }

        const html = div.html();
        ko.cleanNode(div[0]);
        div.remove();
        return html;
    }
    
    static flattenObj(obj: any, parentKey: string, res = {}) {
        for (let key in obj) {
            const propName = parentKey ? parentKey + "." + key : key;
            const value = obj[key];

            if (typeof value === "object") {
                genUtils.flattenObj(value, propName, res);
            } else {
                (<any>res)[propName] = value;
            }
        }
        return res;
    }

    // Trim the specified properties for the object param passed. Return the trimmed object.
    static trimProperties<T extends object>(obj: T, propsToTrim: (OnlyStrings<T>)[]): T {
        if (!obj) {
            return null;
        }

        const result = {} as T;

        for (const key of Object.keys(obj)) {
            const value = (obj as any)[key];

            if (_.includes(propsToTrim, key)) {
                (result as any)[key] = value?.toString().trim();
            } else {
                (result as any)[key] = value;
            }
        }

        return result;
    }
} 

export = genUtils;
