//Modified version of https://gist.github.com/1195667
if (!Array.prototype.Map) {
  Array.prototype.Map = function(callback, thisArg) {
    var T, k;
    if (this == null) {
      throw new TypeError( " this is null or not defined" );
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