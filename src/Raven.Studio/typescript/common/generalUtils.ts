/// <reference path="../../typings/tsd.d.ts" />

class genUtils {
    
    static formatAsCommaSeperatedString(input, digitsAfterDecimalPoint) {
        var parts = input.toString().split(".");
        parts[0] = parts[0].replace(/\B(?=(\d{3})+(?!\d))/g, ",");

        if (parts.length == 2 && parts[1].length > digitsAfterDecimalPoint) {
            parts[1] = parts[1].substring(0, digitsAfterDecimalPoint);
        }
        return parts.join(".");
    }

    static formatTimeSpan(input: string) {
        var timeParts = input.split(":");
        var miliPart;
        var sec = 0, milisec = 0;
        if (timeParts.length == 3) {
            miliPart = timeParts[2].split(".");
            sec = parseInt(miliPart[0]);
            var tmpMili;
            if (miliPart[1][0] == '0') {
                tmpMili = miliPart[1].substring(1, 3);
            } else {
                tmpMili = miliPart[1].substring(0, 3);
            }
            milisec = parseInt(tmpMili);
        }
        var hours = parseInt(timeParts[0]);
        var min = parseInt(timeParts[1]);

        var timeStr = "";
        if (hours > 0) {
            timeStr = hours + " Hours ";
        }
        if (min > 0) {
            timeStr += min + " Min ";
        }
        if (sec > 0) {
            timeStr += sec + " Sec ";
        }
        if ((timeStr == "") && (milisec > 0)) {
            timeStr = milisec + " Ms ";
        }
        return timeStr;
    }

    static formatBytesToSize(bytes: number) {
        var sizes = ['Bytes', 'KB', 'MB', 'GB', 'TB'];
        if (bytes == 0) return 'n/a';
        var i = Math.floor(Math.log(bytes) / Math.log(1024));

        if (i < 0) {
            // number < 1
            return genUtils.formatAsCommaSeperatedString(bytes, 4) + ' Bytes';
        }

        var res = bytes / Math.pow(1024, i);
        var newRes = genUtils.formatAsCommaSeperatedString(res, 2);

        return newRes + ' ' + sizes[i];
    }

    // replace characters with their char codes, but leave A-Za-z0-9 and - in place. 
    static escape(input) {
        var output = "";
        for (var i = 0; i < input.length; i++) {
            var ch = input.charCodeAt(i);
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
} 
export = genUtils;
