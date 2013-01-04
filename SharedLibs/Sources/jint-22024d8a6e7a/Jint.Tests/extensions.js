if (!Array.prototype.lastIndexOf) {
    Array.prototype.lastIndexOf = function(elt /*, from*/) {
        var len = this.length;

        var from = Number(arguments[1]);
        if (isNaN(from)) {
            from = len - 1;
        }
        else {
            from = (from < 0)
           ? Math.ceil(from)
           : Math.floor(from);
            if (from < 0)
                from += len;
            else if (from >= len)
                from = len - 1;
        }

        for (; from > -1; from--) {
            if (from in this &&
          this[from] === elt)
                return from;
        }
        return -1;
    };
}

if (!Array.prototype.forEach) {
    Array.prototype.forEach = function(fun /*, thisp*/) {
        var len = this.length >>> 0;
        if (typeof fun != "function")
            throw new TypeError();

        var thisp = arguments[1];
        for (var i = 0; i < len; i++) {
            if (i in this)
                fun.call(thisp, this[i], i, this);
        }
    };
}

if (!Array.prototype.map) {
    Array.prototype.map = function(fun /*, thisp*/) {
        var len = this.length >>> 0;
        if (typeof fun != "function")
            throw new TypeError();

        var res = new Array(len);
        var thisp = arguments[1];
        for (var i = 0; i < len; i++) {
            if (i in this)
                res[i] = fun.call(thisp, this[i], i, this);
        }

        return res;
    };
}

if (!Array.prototype.reduce) {
    Array.prototype.reduce = function(fun /*, initial*/) {
        var len = this.length >>> 0;
        if (typeof fun != "function")
            throw new TypeError();

        // no value to return if no initial value and an empty array
        if (len == 0 && arguments.length == 1)
            throw new TypeError();

        var i = 0;
        if (arguments.length >= 2) {
            var rv = arguments[1];
        }
        else {
            do {
                if (i in this) {
                    rv = this[i++];
                    break;
                }

                // if array contains no values, no initial value to return
                if (++i >= len)
                    throw new TypeError();
            }
            while (true);
        }

        for (; i < len; i++) {
            if (i in this)
                rv = fun.call(null, rv, this[i], i, this);
        }

        return rv;
    };
}

if (!Array.prototype.indexOf) {
    Array.prototype.indexOf = function(elt /*, from*/) {
        var len = this.length >>> 0;

        var from = Number(arguments[1]) || 0;
        from = (from < 0)
         ? Math.ceil(from)
         : Math.floor(from);
        if (from < 0)
            from += len;

        for (; from < len; from++) {
            if (from in this &&
          this[from] === elt)
                return from;
        }
        return -1;
    };
}

if (!Array.prototype.some) {
    Array.prototype.some = function(fun /*, thisp*/) {
        var i = 0,
        len = this.length >>> 0;

        if (typeof fun != "function")
            throw new TypeError();

        var thisp = arguments[1];
        for (; i < len; i++) {
            if (i in this &&
          fun.call(thisp, this[i], i, this))
                return true;
        }

        return false;
    };
}

if (!Array.prototype.every) {
    Array.prototype.every = function(fun /*, thisp*/) {
        var len = this.length >>> 0;
        if (typeof fun != "function")
            throw new TypeError();

        var thisp = arguments[1];
        for (var i = 0; i < len; i++) {
            if (i in this &&
          !fun.call(thisp, this[i], i, this))
                return false;
        }

        return true;
    };
}
