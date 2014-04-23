define(function(require, exports, module) {
"use strict";

var oop = require("../lib/oop");
var lang = require("../lib/lang");
var TextHighlightRules = require("./text_highlight_rules").TextHighlightRules;

var LuceneHighlightRules = function () {

    var keywordMapper = this.createKeywordMapper({        
        "logics": "AND|OR|NOT"
    }, "identifier");

    this.$rules = {
        "start" : [
            {
                token : "constant.character.negation",
                regex : "[\\-]"
            }, {
                token : "constant.character.interro",
                regex : "[\\?]"
            }, {
                token : "constant.character.asterisk",
                regex : "[\\*]"
            }, {
                token: 'constant.character.proximity',
                regex: '~[0-9]+\\b'
            }, {
                token : 'keyword.operator',
                regex: '(?:AND|OR|NOT)\\b'
            }, {
                token : "paren.lparen",
                regex : "[\\(]"
            }, {
                token : "paren.rparen",
                regex : "[\\)]"
            }, {
                token : "keyword",
                regex: "[\\S]+:",
                next: "value"
            }, {
                token : "string",           // " string
                regex : '".*?"'
            }, {
                token : "text",
                regex : "\\s+"
            }
        ],
        "value": [
            {
                token: "value",                
                regex: '\\s+'
            },
            {
                token: "value",
                //regex: '(\\"[^\\"]*\\")|([^\\s\\"]+\\s)',
                regex: '[\\"]',
                next: "valueQuotCont"
            },
            {
                token: "value",
                //regex: '(\\"[^\\"]*\\")|([^\\s\\"]+\\s)',
                regex: '[^\\"\\s]',
                next: "valueNonQuotCont"
            }
        ],
        "valueQuotCont": [
            {
                token: "value",
                regex: '[^\\"]+'
            },
            {
                token: "value",
                regex: '[\\"]',
                next: "start"
            }
        ],
        "valueNonQuotCont": [           
            {
                token: "value",
                regex: '[^\\"\\s]*',
                next: "start"
            }
        ]
    };
};

oop.inherits(LuceneHighlightRules, TextHighlightRules);

exports.LuceneHighlightRules = LuceneHighlightRules;
});
