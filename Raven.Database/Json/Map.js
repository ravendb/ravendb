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