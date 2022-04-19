define(function(require, exports, module) {
    "use strict";

    var check = function(key, at, warnings) {
        var isNumeric = x => !isNaN(x) && !isNaN(parseFloat(x));

        if (isNumeric(key)) {
            warnings.push({
                text: "Object contains numeric keys. The order of these keys might change after saving the document.",
                at: at
            })
        }
    };
    
    exports.Check = check;

});
