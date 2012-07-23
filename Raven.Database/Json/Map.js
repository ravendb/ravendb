//Modified version of https://gist.github.com/1195667
if (!Array.prototype.Map) {
	Array.prototype.Map = function (callback, thisArg) {
		var T, k;
		if (this == null) {
			throw new TypeError(" this is null or not defined");
		}
		var O = Object(this);
		var len = O.length >>> 0;
		if ({}.toString.call(callback) != "[object Function]") {
			throw new TypeError(callback + " is not a function");
		}
		if (thisArg) {
			T = thisArg;
		}
		k = 0;

		if (len > 10000)
			throw 'Map was called on a collection with ' + len + ' items, which is more than the max of 10,000 items allowed';

		while (k < len) {
			var kValue;
			if (k in O) {
				kValue = O[k];
				//allow the function to return the new value!!
				O[k] = callback.call(T, kValue, k, O);
			}
			k++;
		}
	};
}

if (!Array.prototype.indexOf) {
	Array.prototype.indexOf = function (elt /*, from*/) {
		var len = this.length;

		var from = Number(arguments[1]) || 0;
		from = (from < 0)
         ? Math.ceil(from)
         : Math.floor(from);
		if (from < 0)
			from += len;

		for (; from < len; from++) {
			if (from in this && this[from] === elt)
				return from;
		}
		return -1;
	};
}

if (!Array.prototype.filter) {
	Array.prototype.filter = function (fun /*, thisp*/) {
		var len = this.length;
		if (typeof fun != "function")
			throw new TypeError();

		var res = new Array();
		var thisp = arguments[1];
		for (var i = 0; i < len; i++) {
			if (i in this) {
				var val = this[i]; // in case fun mutates this
				if (fun.call(thisp, val, i, this))
					res.push(val);
			}
		}

		return res;
	};
}