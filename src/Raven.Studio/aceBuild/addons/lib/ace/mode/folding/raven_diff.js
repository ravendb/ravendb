define(function(require, exports, module) {
    "use strict";

    var Range = require("../../range").Range;

    var FoldMode = exports.FoldMode = function() {};

    (function() {
        // must return "" if there's no fold, to enable caching
        this.getFoldWidget = function(session, foldStyle, row) {
            
            var rowDecorations = session.$decorations[row] || "";
            
            if (rowDecorations.includes("diff_fold")) {
                return "start";
            }
            
            return "";
        };

        this.getFoldWidgetRange = function(session, foldStyle, row) {
            var rowDecorations = session.$decorations[row] || "";
            var foldPrefix = "diff_l_";
            var foldLength = rowDecorations.split(" ").find(function (value, index) { 
                return value.startsWith(foldPrefix);
            });
            
            var length = parseInt(foldLength.substr(foldPrefix.length));
            
            return new Range(row, 0, row + length - 1, Infinity);
        };

    }).call(FoldMode.prototype);

});
