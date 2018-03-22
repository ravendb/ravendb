//https://developer.mozilla.org/en-US/docs/Web/JavaScript/Reference/Global_Objects/String/startsWith
if (!String.prototype.startsWith) {
    String.prototype.startsWith = function(searchString, position){
      return this.substr(position || 0, searchString.length) === searchString;
  };
}
//https://developer.mozilla.org/en-US/docs/Web/JavaScript/Reference/Global_Objects/String/endsWith
if (!String.prototype.endsWith) {
    String.prototype.endsWith = function (searchStr, position) {
        if (!(position < this.length))
          position = this.length;
      else
            position |= 0; // round position
      return this.substr(position - searchStr.length,
                         searchStr.length) === searchStr;
    };
}
//https://github.com/KhaledElAnsari/String.prototype.padStart/blob/master/index.js
if (!String.prototype.padStart) {
    String.prototype.padStart = function(targetLength, padString) {
        targetLength = Math.floor(targetLength) || 0;
        if(targetLength < this.length) return String(this);

        padString = padString ? String(padString) : " ";

        var pad = "";
        var len = targetLength - this.length;
        var i = 0;
        while(pad.length < len) {
            if(!padString[i]) {
                i = 0;
            }
            pad += padString[i];
            i++;
        }

        return pad + String(this).slice(0);
    };
}
//https://github.com/KhaledElAnsari/String.prototype.padEnd/blob/master/index.js
if (!String.prototype.padEnd) {
    String.prototype.padEnd = function(targetLength, padString) {
        targetLength = Math.floor(targetLength) || 0;
        if(targetLength < this.length) return String(this);

        padString = padString ? String(padString) : " ";

        var pad = "";
        var len = targetLength - this.length;
        var i = 0;
        while(pad.length < len) {
            if(!padString[i]) {
                i = 0;
            }
            pad += padString[i];
            i++;
        }

        return String(this).slice(0) + pad;
    };
}
//https://github.com/jsPolyfill/Array.prototype.find/blob/master/find.js
if (!Array.prototype.find) {
    Array.prototype.find = Array.prototype.find || function(callback) {
      if (this === null) {
        throw new TypeError('Array.prototype.find called on null or undefined');
      } else if (typeof callback !== 'function') {
        throw new TypeError('callback must be a function');
      }
      var list = Object(this);
      // Makes sures is always has an positive integer as length.
      var length = list.length >>> 0;
      var thisArg = arguments[1];
      for (var i = 0; i < length; i++) {
        var element = list[i];
        if ( callback.call(thisArg, element, i, list) ) {
          return element;
        }
      }
    };
}

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


