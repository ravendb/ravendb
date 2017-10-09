/// <reference path="../../typings/tsd.d.ts" />

import pluralizeHelpers = require("common/helpers/text/pluralizeHelpers");

class genUtils {

    /***  Date-Time Methods  ***/

    static timeSpanAsAgo(input: string, withSuffix: boolean): string {
        if (!input) {
            return null;
        }

        return moment.duration("-" + input).humanize(withSuffix);
    }

    static formatDuration(duration: moment.Duration, longFormat = false) {
        let timeStr = "";
        if (duration.asDays() >= 1) {
            timeStr += longFormat ?
                pluralizeHelpers.pluralize(Math.floor(duration.asDays()), " day ", " days ") :
                Math.floor(duration.asDays()) + " d ";
        }
        if (duration.hours() > 0) {
            timeStr += longFormat ?
                pluralizeHelpers.pluralize(duration.hours(), " hour ", " hours ") :
                duration.hours() + " h ";
        }
        if (duration.minutes() > 0) {
            timeStr += longFormat ?
                pluralizeHelpers.pluralize(duration.minutes(), " minute ", " minutes ") :
                duration.minutes() + " m ";
        }
        if (duration.seconds() > 0) {
            timeStr += longFormat ?
                pluralizeHelpers.pluralize(duration.seconds(), " second ", " seconds ") :
                duration.seconds() + " s ";
        }
        if (duration.milliseconds() > 0) {
            const millis = duration.milliseconds();

            const atLeastOneSecond = duration.asSeconds() >= 1;

            timeStr += atLeastOneSecond ? Math.floor(millis) : Math.floor(millis * 100) / 100;
            timeStr += " ms";
        }
        return timeStr;
    }

    static formatMillis(input: number) {
        return genUtils.formatDuration(moment.duration({
            milliseconds: input
        }));
    }

    static formatTimeSpan(input: string | number, longFormat = false) {
        return genUtils.formatDuration(moment.duration(input), longFormat);
    }

    static timeSpanToSeconds(input: string) {
        return moment.duration(input).asSeconds();
    }

    static formatAsTimeSpan(millis: number) {
        const duration = moment.duration({
            milliseconds: millis
        });

        const formatNumber = (input: number) => _.padStart(input.toString(), 2, '0');

        if (duration.days()) {
            return `${duration.days()}.${formatNumber(duration.hours())}:${formatNumber(duration.minutes())}:${formatNumber(duration.seconds())}`;
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
    static formatBytesToSize(bytes: number) : string {
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
            return genUtils.formatAsCommaSeperatedString(bytes, 4) + ' Bytes';
        }

        const res = bytes / Math.pow(1024, i);
        const newRes = genUtils.formatAsCommaSeperatedString(res, 2);

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

    static getSizeClass(input: number) : string {
        if (input < 100000) {
            return "";
        }
        if (input < 1000 * 1000) {
            return "kilo";
        }
        return "mega";
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

    /***  String Methods ***/

    static trimMessage(message: any) {
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

        if (message.length < 256) {
            return message;
        }

        return message.substr(0, 256) + "...";
    }
    
    static formatAsCommaSeperatedString(input: number, digitsAfterDecimalPoint: number) {
        const parts = input.toString().split(".");
        parts[0] = parts[0].replace(/\B(?=(\d{3})+(?!\d))/g, ",");

        if (parts.length == 2 && parts[1].length > digitsAfterDecimalPoint) {
            parts[1] = parts[1].substring(0, digitsAfterDecimalPoint);
        }
        return parts.join(".");
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

    // Return the inputNumber as a string with separating commas rounded to 'n' decimal digits
    // (e.g. for n==2: 2046430.45756 => "2,046,430.46")
    static formatNumberToStringFixed(inputNumber: number, n: number): string {
        return inputNumber.toLocaleString(undefined, { minimumFractionDigits: n, maximumFractionDigits: n });
    };

    /***  Other Methods ***/

    static debounceAndFunnel<T>(func: (val: T, params: any, callback: (currentValue: T, result: boolean) => void) => void) {
        return _.debounce((val: T, params: any, callback: (result: boolean) => void) => {
            func(val, params, (currentValue, result) => {
                if (currentValue === val) {
                    callback(result);
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
    
    static findChanges<T, K>(oldArray: T[], newArray: T[], idProvider: (item: T) => K): { 
        enter: T[],
        change: { from: T, to: T }[],
        exit: T[]
    } {
        const enter = [] as T[];
        const change = [] as { from: T, to: T }[];
        
        const oldTableLookup = new Map<K, T>();
        for (let i = 0; i < oldArray.length; i++) {
            const id = idProvider(oldArray[i]);
            oldTableLookup.set(id, oldArray[i]);
        }
        
        for (let i = 0; i < newArray.length; i++) {
            const item = newArray[i];
            const id = idProvider(item);
            if (oldTableLookup.has(id)) {
                // change
                change.push({
                    from: oldTableLookup.get(id),
                    to: item
                });
                oldTableLookup.delete(id)
            } else {
                // new
                enter.push(item);
            }
        }
        
        // remaing items are deleted items
        const exit = Array.from(oldTableLookup.values());
        
        return {
            enter: enter, 
            change: change, 
            exit: exit
        }
    } 
} 

export = genUtils;
