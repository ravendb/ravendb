define(function(require, exports, module) {
    "use strict";

    var check = function(number, at, warnings) {
        if (number > Number.MAX_SAFE_INTEGER) {
            warnings.push({
                text: "Numeric value is greater than " + Number.MAX_SAFE_INTEGER + " (Number.MAX_SAFE_INTEGER). \r\nSaving the document might cause a precision loss.",
                at: at
            });
        }
        
        if (number < Number.MIN_SAFE_INTEGER) {
            warnings.push({
                text: "Numeric value is less than " + Number.MAX_SAFE_INTEGER + " (Number.MIN_SAFE_INTEGER). \r\nSaving the document might cause a precision loss.",
                at: at
            });
        }
    };
    
    exports.Check = check;

});
