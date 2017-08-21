ace.define("ace/mode/rql_highlight_rules",["require","exports","module","ace/lib/oop","ace/mode/text_highlight_rules"], function(require, exports, module) {
"use strict";

var oop = require("../lib/oop");
var TextHighlightRules = require("./text_highlight_rules").TextHighlightRules;

var RqlHighlightRules = function() {

    var keywords = (
        "select|from|where|and|or|group|by|order|as|desc|asc|not|null|index|in"
    );

    var builtinConstants = (
        "true|false"
    );

    var builtinFunctions = (
        "count|sum|key|search|boost|startsWith|endsWith|lucene|exists|exact|random|score"
    );

    var dataTypes = (
        "long|double|string"
    );

    var keywordMapper = this.createKeywordMapper({
        "support.function": builtinFunctions,
        "keyword": keywords,
        "constant.language": builtinConstants,
        "storage.type": dataTypes
    }, "identifier", true);

    this.$rules = {
        "start" : [ {
            token : "comment",
            regex : "--.*$"
        },  {
            token : "comment",
            start : "/\\*",
            end : "\\*/"
        }, {
            token : "string",           // " string
            regex : '"[^"]*"?'
        }, {
            token : "string",           // ' string
            regex : "'[^']*'?"
        }, {
            token : "string",           // ` string (apache drill)
            regex : "`[^`]*`?"
        }, {
            token : "constant.numeric", // float
            regex : "[+-]?\\d+(?:(?:\\.\\d*)?(?:[eE][+-]?\\d+)?)?\\b"
        }, {
            token : keywordMapper,
            regex : "[a-zA-Z_$.][a-zA-Z0-9_$.]*\\b"
        }, {
            token : "keyword.operator",
            regex : "\\+|\\-|\\/|\\/\\/|%|<@>|@>|<@|&|\\^|~|<|>|<=|=>|==|!=|<>|="
        }, {
            token : "paren.lparen",
            regex : "[\\(]"
        }, {
            token : "paren.rparen",
            regex : "[\\)]"
        }, {
            token : "text",
            regex : "\\s+"
        } ]
    };
    this.normalizeRules();
};

oop.inherits(RqlHighlightRules, TextHighlightRules);

exports.RqlHighlightRules = RqlHighlightRules;
});

ace.define("ace/mode/rql",["require","exports","module","ace/lib/oop","ace/mode/text","ace/mode/rql_highlight_rules"], function(require, exports, module) {
"use strict";

var oop = require("../lib/oop");
var TextMode = require("./text").Mode;
var RqlHighlightRules = require("./rql_highlight_rules").RqlHighlightRules;

var Mode = function() {
    this.HighlightRules = RqlHighlightRules;
    this.$behaviour = this.$defaultBehaviour;
};
oop.inherits(Mode, TextMode);

(function() {

    this.lineCommentStart = "--";

    this.$id = "ace/mode/rql";
}).call(Mode.prototype);

exports.Mode = Mode;

});
