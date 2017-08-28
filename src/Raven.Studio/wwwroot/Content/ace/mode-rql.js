ace.define("ace/mode/rql_highlight_rules",["require","exports","module","ace/lib/oop","ace/mode/text_highlight_rules"], function(require, exports, module) {
"use strict";

var oop = require("../lib/oop");
var TextHighlightRules = require("./text_highlight_rules").TextHighlightRules;

var RqlHighlightRules = function() {

    var clausesKeywords = (
        "declare|function|from|index|where|select|group|order|by|desc|asc|load|include"
    );
    var insideClauseKeywords = (
        "as|and|or|not|all"
    );
    var functions = (
        "count|sum|id|key"
    );
    var whereFunctions = (
        "in|search|boost|startsWith|endsWith|lucene|exact"
    );
    var orderByFunctions = (
        "random|score"
    );

    var constants = (
        "null"
    );

    var constantsBoolean = (
        "true|false"
    );

    var dataTypes = (
        "long|double|string"
    );

    var keywordMapper = this.createKeywordMapper({
        "keyword.clause": clausesKeywords,
        "keyword.insideClause": insideClauseKeywords,
        "function": functions,
        "function.where": whereFunctions,
        "function.orderBy": orderByFunctions,
        "constant.language": constants,
        "constant.language.boolean": constantsBoolean,
        "storage.type": dataTypes
    }, "identifier", true);

    this.$rules = {
        "start" : [ {
            token : "comment",
            regex : "//.*$"
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
            regex : "[a-zA-Z_$@][a-zA-Z0-9_$@]*\\b"
        }, {
            token : "keyword.operator",
            regex : "\\+|\\-|\\/|\\/\\/|%|<@>|@>|<@|&|\\^|~|<|>|<=|=>|==|!=|<>|="
        }, {
            token : "paren.lparen",
            regex : /[\[({]/
        }, {
            token : "paren.rparen",
            regex : /[\])}]/
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

    this.lineCommentStart = "//";
    this.blockComment = {start: "/*", end: "*/"};
    this.$quotes = {'"': '"', "'": "'", "`": "`"};

    this.$id = "ace/mode/rql";
}).call(Mode.prototype);

exports.Mode = Mode;

});
