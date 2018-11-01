define(function(require, exports, module) {
    "use strict";

    var oop = require("../lib/oop");
    var DocCommentHighlightRules = require("./doc_comment_highlight_rules").DocCommentHighlightRules;
    var TextHighlightRules = require("./text_highlight_rules").TextHighlightRules;

    var RavenMapLinqHighlightRules = function () {
        var keywordMapper = this.createKeywordMapper({
            "variable.language": "this|docs|results|" +
                "byte|fixed|float|uint|char|ulong|int|sbyte|short|is|sizeof|double|long|string|String|decimal|Key|Sum|MultiSelect",
            "keyword": "group|by|from|let|select|new|into|null|object|this|bool|out|if|unsafe|ushort|in|decimal|else",
            "constant.language": "null",
            "constant.language.boolean": "true|false"
        }, "identifier");

        // regexp must not have capturing parentheses. Use (?:) instead.
        // regexps are ordered -> the first match is used

        this.$rules = {

            "from.in.col.suffix": [
                {
                    token: "collectionName",
                    regex: "[^\\n\\s\\t]*",
                    next: "start"
                }
            ],

            "from.in.col.prefix": [
                {
                    token: "docs",
                    regex: "[\\s]*(docs).",
                    next:"from.in.col.suffix"
                },
                {
                    token: "collections",
                    regex: "[\\s]*([\\w]+).",
                    next: "from.in.col.suffix"
                }
            ],
            "from.in": [
                {
                    token: "keyword",
                    regex: "\s"
                },
                {
                    token: "keyword",
                    regex: "(?:in)",
                    next: "from.in.col.prefix"
                }
            ],

            "from.alias": [

                {
                    token: "from.alias",
                    regex: "[\\w]+",
                    next:"from.in"
                }

            ],
            "data.prefix": [
                {
                    token: "data.suffix",
                    regex: "[^\\s,]*",
                    next:"start"
                }
            ],
            "start": [
                {
                    token: "comment",
                    regex: "\\/\\/.*$"
                },
                {
                    token: "keyword",
                    regex: "from",
                    next: "from.alias"

                },
                {
                    token: "data.prefix",
                    regex: "\\w+\\.",
                    next: "data.prefix"

                },
                DocCommentHighlightRules.getStartRule("doc-start"),
                {
                    token: "comment", // multi line comment
                    regex: "\\/\\*",
                    next: "comment"
                }, {
                    token: "string.regexp",
                    regex: "[/](?:(?:\\[(?:\\\\]|[^\\]])+\\])|(?:\\\\/|[^\\]/]))*[/]\\w*\\s*(?=[).,;]|$)"
                }, {
                    token: "string", // character
                    regex: /'(?:.|\\(:?u[\da-fA-F]+|x[\da-fA-F]+|[tbrf'"n]))'/
                }, {
                    token: "string", start: '"', end: '"|$', next: [
                        { token: "constant.language.escape", regex: /\\(:?u[\da-fA-F]+|x[\da-fA-F]+|[tbrf'"n])/ },
                        { token: "invalid", regex: /\\./ }
                    ]
                }, {
                    token: "string", start: '@"', end: '"', next: [
                        { token: "constant.language.escape", regex: '""' }
                    ]
                }, {
                    token: "constant.numeric", // hex
                    regex: "0[xX][0-9a-fA-F]+\\b"
                }, {
                    token: "constant.numeric", // float
                    regex: "[+-]?\\d+(?:(?:\\.\\d*)?(?:[eE][+-]?\\d+)?)?\\b"
                }, {
                    token: "constant.language.boolean",
                    regex: "(?:true|false)\\b"
                }, {
                    token: keywordMapper,
                    regex: "[a-zA-Z_$][a-zA-Z0-9_$]*\\b"
                }, {
                    token: "keyword.operator",
                    regex: "!|\\$|%|&|\\*|\\-\\-|\\-|\\+\\+|\\+|~|===|==|=|!=|!==|<=|>=|<<=|>>=|>>>=|<>|<|>|!|&&|\\|\\||\\?\\:|\\*=|%=|\\+=|\\-=|&=|\\^=|\\b(?:in|instanceof|new|delete|typeof|void)"
                }, {
                    token: "keyword",
                    regex: "^\\s*#(if|else|elif|endif|define|undef|warning|error|line|region|endregion|pragma)"
                }, {
                    token: "punctuation.operator",
                    regex: "\\?|\\:|\\,|\\;|\\."
                }, {
                    token: "paren.lparen",
                    regex: "[[({]"
                }, {
                    token: "paren.rparen",
                    regex: "[\\])}]"
                }, {
                    token: "text",
                    regex: "\\s+"
                }
            ],
            "comment": [
                {
                    token: "comment", // closing comment
                    regex: ".*?\\*\\/",
                    next: "start"
                }, {
                    token: "comment", // comment spanning whole line
                    regex: ".+"
                }
            ]
        };

        this.embedRules(DocCommentHighlightRules, "doc-",
            [DocCommentHighlightRules.getEndRule("start")]);
        this.normalizeRules();
    };

    oop.inherits(RavenMapLinqHighlightRules, TextHighlightRules);

    exports.RavenMapLinqHighlightRules = RavenMapLinqHighlightRules;
});
