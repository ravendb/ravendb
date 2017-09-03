ace.define("ace/mode/rql_highlight_rules",["require","exports","module","ace/lib/oop","ace/mode/text_highlight_rules"], function(require, exports, module) {
"use strict";

var oop = require("../lib/oop");
var TextHighlightRules = require("./text_highlight_rules").TextHighlightRules;

var RqlHighlightRules = function() {

    var clausesKeywords = (
        "declare|from|where|select|group|order|load|include|update"
    );
    var clauseAppendKeywords = (
        "function|index|by"
    );
    var insideClauseKeywords = (
        "as|not|all|between"
    );
    var functions = (
        "count|sum|id|key"
    );
    var whereFunctions = (
        "in|search|boost|startsWith|endsWith|lucene|exact|within|circle"
    );
    var orderByFunctions = (
        "random|score"
    );
    
    var orderByOptions = (
        "desc|asc|descending|ascending"
    );
    var orderByAsOptions = (
        "string|long|double|alphaNumeric"
    );

    var constants = (
        "null"
    );
    var constantsBoolean = (
        "true|false"
    );
    var binaryOperations = (
        "and|or"
    );
    var operations = (
        ">=|<=|<|>|=|==|!="
    );

    var keywordMapper = this.createKeywordMapper({
        "keyword.clause": clausesKeywords,
        "keyword.clause.clauseAppend": clauseAppendKeywords,
        "keyword.insideClause": insideClauseKeywords,
        "keyword.orderByOptions": orderByOptions,
        "keyword.orderByAsOptions": orderByAsOptions,
        "function": functions,
        "function.where": whereFunctions,
        "function.orderBy": orderByFunctions,
        "constant.language": constants,
        "constant.language.boolean": constantsBoolean,
        "operations.type.binary": binaryOperations,
        "operations.type": operations
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

    this.clauseAppendKeywords = clauseAppendKeywords.split("|");
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
    this.prefixRegexps = [/[a-zA-Z_0-9.'"\\\/\$\-\u00A2-\uFFFF]/]
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
