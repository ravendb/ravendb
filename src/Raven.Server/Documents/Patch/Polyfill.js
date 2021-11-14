//format
if (!String.prototype.format) {
    String.prototype.format = function () {
        var args = arguments;
        return this.replace(/{(\d+)}/g, function (match, number) {
            return typeof args[number] != 'undefined'
                    ? args[number]
                    : match;
        });
    };
}

//map on objects
Object.map = function (o, f, ctx) {
    ctx = ctx || this;
    var result = [];
    Object.keys(o).forEach(function(k) {
        result.push(f.call(ctx, o[k], k));
    });
    return result;
};


var isUtc = dateStr => dateStr.endsWith("Z") || dateStr.endsWith("+00:00") || dateStr.endsWith("-00:00") || 
    dateStr.endsWith("GMT") || dateStr.includes("GMT+0") || dateStr.includes("GMT-0")

if (!String.prototype.getFullYear) {
    String.prototype.getFullYear = function () {
        let d = new Date(Date.parse(this));
        return isUtc(this) ? d.getUTCFullYear() : d.getFullYear();
    };
}

if (!String.prototype.getMonth) {
    String.prototype.getMonth = function () {
        let d = new Date(Date.parse(this));
        return isUtc(this) ? d.getUTCMonth() : d.getMonth();
    };
}

if (!String.prototype.getDate) {
    String.prototype.getDate = function () {
        let d = new Date(Date.parse(this));
        return isUtc(this) ? d.getUTCDate() : d.getDate();
    };
}

if (!String.prototype.getHours) {
    String.prototype.getHours = function () {
        let d = new Date(Date.parse(this));
        return isUtc(this) ? d.getUTCHours() : d.getHours();
    };
}

if (!String.prototype.getMinutes) {
    String.prototype.getMinutes = function () {
        let d = new Date(Date.parse(this));
        return isUtc(this) ? d.getUTCMinutes() : d.getMinutes();
    };
}

if (!String.prototype.getSeconds) {
    String.prototype.getSeconds = function () {
        let d = new Date(Date.parse(this));
        return isUtc(this) ? d.getUTCSeconds() : d.getSeconds();
    };
}

if (!String.prototype.getMilliseconds) {
    String.prototype.getMilliseconds = function () {
        let d = new Date(Date.parse(this));
        return isUtc(this) ? d.getUTCMilliseconds() : d.getMilliseconds();
    };
}

if (!String.prototype.getTime) {
    String.prototype.getTime = function () {
        let d = new Date(Date.parse(this));
        return d.getTime();
    };
}
