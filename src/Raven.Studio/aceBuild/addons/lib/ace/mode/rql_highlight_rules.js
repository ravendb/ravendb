define(function(require, exports, module) {
    "use strict";

    var oop = require("../lib/oop");
    var TextHighlightRules = require("./text_highlight_rules").TextHighlightRules;
    var JavaScriptHighlightRules = require("./javascript_highlight_rules").JavaScriptHighlightRules;

    var RqlHighlightRules = function() {

        var keywordRegex = /[a-zA-Z_$@\u00a1-\uffff][a-zA-Z0-9_$@\u00a1-\uffff]*\b/;

        var clausesKeywords = (
            "declare|from|group|where|order|load|select|include|update|match|with"
        );
        this.clausesKeywords = clausesKeywords.split("|");

        var clauseAppendKeywords = (
            "function|index|by"
        );
        this.clauseAppendKeywords = clauseAppendKeywords.split("|");

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
            "keyword.asKeyword": "as",
            "keyword.notKeyword": "not",
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

        var curelyBracesCount = 0;

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
            regex : /{/,
            next: function (currentState, stack) {
                curelyBracesCount++;
                return "js-start";
            }
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
            token :  "field",
            regex : /[a-zA-Z_$@\u00a1-\uffff][a-zA-Z0-9_$@\u00a1-\uffff]*(?:\[\])?\.[a-zA-Z0-9_$@\u00a1-\uffff.]*/
        }, {
            token :  "function.where",
            regex : whereFunctions,
            next: "whereFunction"
        }, {
            token : keywordMapper,
            regex : keywordRegex
        }, {
            token : "operator.where",
            regex : /(?:==|!=|>=|<=|=|<>|>|<)(?=\s)/
        }, {
            token : "paren.rparen",
            regex : /[\])}]/
        } ];

        var whereFunctionsRules = [ {
            token : "identifier",
            regex : keywordRegex
        }, {
            token : "paren.rparen",
            regex : /[)]/,
            next: "start"
        } ];

        this.$rules = {
            "start" : commonRules.concat(startRule),
            "whereFunction" : commonRules.concat(whereFunctionsRules).map(function (rule) {
                return {
                    token: rule.token + ".whereFunction",
                    regex: rule.regex,
                    start: rule.start,
                    end: rule.end,
                    next: rule.next
                };
            })
        };

        this.embedRules(JavaScriptHighlightRules, "js-", [ {
            token : function (value, currentState, stack) {
                if (currentState === "js-string.quasi.start") {
                    return "string.quasi.start";
                }
                if (currentState === "js-qqstring" || currentState === "js-qstring") {
                    return "string";
                }
                curelyBracesCount++;
                return "paren.lparen";
            },
            regex: /{/
        }, {
            token : function (value, currentState, stack) {
                if (currentState !== "js-start" && currentState !== "js-no_regex") {
                    return "string";
                }
                return "paren.rparen";
            },
            regex : /}/,
            next : function (currentState, stack) {
                if (currentState !== "js-start" && currentState !== "js-no_regex") {
                    return currentState;
                }
                if (--curelyBracesCount > 0) {
                    return currentState;
                }
                return "start";
            }
        }]);

        this.normalizeRules();
    };

    oop.inherits(RqlHighlightRules, TextHighlightRules);

    exports.RqlHighlightRules = RqlHighlightRules;
});
