ace.define("ace/mode/rql_highlight_rules",["require","exports","module","ace/lib/oop","ace/mode/text_highlight_rules"], function(require, exports, module) {
"use strict";

var oop = require("../lib/oop");
var TextHighlightRules = require("./text_highlight_rules").TextHighlightRules;

var RqlHighlightRules = function() {

    var clausesKeywords = (
        "declare|from|group|where|order|load|select|include|update"
    );
    this.clausesKeywords = clausesKeywords.split("|");

    var clauseAppendKeywords = (
        "function|index|by"
    );
    this.clauseAppendKeywords = clauseAppendKeywords.split("|");

    var insideClauseKeywords = (
        "as|not" // TODO: not should be after AND or OR
    );
    var functions = (
        "count|sum|id|key"
    );

    var whereOperators = (
        "all|in|between"
    );

    var whereFunctions = (
        "search|boost|startsWith|endsWith|lucene|exact|within|exists|contains|disjoint|intersects"
    );
    this.whereFunctions = whereFunctions.split("|");

    var withinFunctions = (
        "circle"
    );
    this.withinFunctions = withinFunctions.split("|");

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
    this.binaryOperations = binaryOperations.split("|");

    var operations = (
        ">=|<=|<|>|=|==|!="
    );

    var keywordMapper = this.createKeywordMapper({
        "keyword.clause": clausesKeywords,
        "keyword.clause.clauseAppend": clauseAppendKeywords,
        "keyword.insideClause": insideClauseKeywords,
        "keyword.orderByOptions": orderByOptions,
        "keyword.orderByAsOptions": orderByAsOptions,
        "keyword.whereOperators": whereOperators,
        "function": functions,
        "function.where.within": withinFunctions,
        "function.orderBy": orderByFunctions,
        "constant.language": constants,
        "constant.language.boolean": constantsBoolean,
        "operations.type.binary": binaryOperations,
        "operations.type": operations
    }, "identifier", true);

    var commonRules = [ {
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
        token : "paren.lparen",
        regex : /[\[({]/
    }, {
        token : "comma",
        regex : /,/
    }, {
        token : "space",
        regex : /\s+/
    } ];
    
    var startRule = [ {
        token :  "function.where",
        regex : whereFunctions,
        next: "whereFunction"
    }, {
        token : keywordMapper,
        regex : "[a-zA-Z_$@][a-zA-Z0-9_$@]*\\b"
    }, {
        token : "operator.where",
        regex : /(?:==|!=|>=|<=|=|<>|>|<)(?=\s)/
    }, {
        token : "paren.rparen",
        regex : /[\])}]/
    } ];
    
    var whereFunctionsRules = [ {
        token : "identifier",
        regex : "[a-zA-Z_$@][a-zA-Z0-9_$@]*\\b"
    }, {
        token : "paren.rparen",
        regex : /[\])}]/,
        next: "start"
    } ];
    
    this.$rules = {
        "start" : commonRules.concat(startRule),    
        "whereFunction" : commonRules.map(function (rule) {
            return {
                token: rule.token + ".whereFunction",
                regex: rule.regex,
                start: rule.start,
                end: rule.end
            };
        }).concat(whereFunctionsRules)
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
    this.prefixRegexps = [/[a-zA-Z_0-9@'"\\\/\$\-\u00A2-\uFFFF=!<>]/]
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
