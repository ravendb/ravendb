var debug_outputs = [];

function output(msg) {
    debug_outputs.push(msg);
}

function dump(obj) {
    debug_outputs.push(JSON.stringify(obj));
}

function clear_debug_outputs() {
	debug_outputs = [];
}

String.prototype.trim = String.prototype.trim || function () {
    return this.replace(/^\s\s*/, '').replace(/\s\s*$/, '');
};

Array.prototype.Map = Array.prototype.Map || function (callback, thisArg) {
    // uncomment these to keep old behavior of throwing if array is null
    //if (this == null) {
    //	throw new TypeError(" this is null or not defined");
    //}

    return _.map(this, callback, thisArg);
};

Array.prototype.indexOf = Array.prototype.indexOf || function (value) {
    return _.indexOf(this, value);
};

Array.prototype.filter = Array.prototype.filter || function(callback, thisArg) {
    return _.filter(this, callback, thisArg);
};

Array.prototype.Where = Array.prototype.filter;

Array.prototype.RemoveWhere = function(callback, thisArg) {
    _.remove(this, callback, thisArg);
    return this;
};

Array.prototype.Remove = function (value) {
    _.pull(this, value);
    return this;
};
