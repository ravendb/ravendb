define("ace/mode/json_newline_friendly_highlight_rules", ["require", "exports", "module", "ace/lib/oop", "ace/mode/text_highlight_rules"], function(require, exports, module) {
"use strict";

var oop = require("../lib/oop");
var TextHighlightRules = require("./text_highlight_rules").TextHighlightRules;

var JsonHighlightRules = function() {

    // regexp must not have capturing parentheses. Use (?:) instead.
    // regexps are ordered -> the first match is used
    this.$rules = {
        "start" : [
            {
                token : "variable", // single line
                regex : '["](?:(?:\\\\.)|(?:[^"\\\\]))*?["]\\s*(?=:)'
            }, {
                token : "string", // single line
                regex : '"',
                next  : "string"
            }, {
                token : "constant.numeric", // hex
                regex : "0[xX][0-9a-fA-F]+\\b"
            }, {
                token : "constant.numeric", // float
                regex : "[+-]?\\d+(?:(?:\\.\\d*)?(?:[eE][+-]?\\d+)?)?\\b"
            }, {
                token : "constant.language.boolean",
                regex : "(?:true|false)\\b"
            }, {
                token: "constant.language.null",
                regex: "(?:null)\\b"
            }, {
                token : "invalid.illegal", // single quoted strings are not allowed
                regex : "['](?:(?:\\\\.)|(?:[^'\\\\]))*?[']"
            }, {
                token : "invalid.illegal", // comments are not allowed
                regex : "\\/\\/.*$"
            }, {
                token : "paren.lparen",
                regex : "[[({]"
            }, {
                token : "paren.rparen",
                regex : "[\\])}]"
            }, {
                token : "text",
                regex : "\\s+"
            }
        ],
        "string" : [
            {
                token : "constant.language.escape",
                regex : /\\(?:x[0-9a-fA-F]{2}|u[0-9a-fA-F]{4}|["\\\/bfnrt])/
            }, {
                token : "string",
                regex : '[^"\\\\]+'
            }, {
                token : "string",
                regex : '"',
                next  : "start"
            }
            
        ]
    };
    
};

oop.inherits(JsonHighlightRules, TextHighlightRules);

exports.JsonHighlightRules = JsonHighlightRules;
});



define("ace/mode/json_newline_friendly", ["require", "exports", "module", "ace/lib/oop", "ace/mode/text", "ace/tokenizer", "ace/mode/json_newline_friendly_highlight_rules", "ace/mode/matching_brace_outdent", "ace/mode/behaviour/cstyle", "ace/mode/folding/cstyle", "ace/worker/worker_client"], function(require, exports, module) {
"use strict";

var oop = require("../lib/oop");
var TextMode = require("./text").Mode;
var Tokenizer = require("../tokenizer").Tokenizer;
var HighlightRules = require("./json_newline_friendly_highlight_rules").JsonHighlightRules;
var MatchingBraceOutdent = require("./matching_brace_outdent").MatchingBraceOutdent;
var CstyleBehaviour = require("./behaviour/cstyle").CstyleBehaviour;
var CStyleFoldMode = require("./folding/cstyle").FoldMode;
var WorkerClient = require("../worker/worker_client").WorkerClient;

var Mode = function() {
    this.HighlightRules = HighlightRules;
    this.$outdent = new MatchingBraceOutdent();
    this.$behaviour = new CstyleBehaviour();
    this.foldingRules = new CStyleFoldMode();
};
oop.inherits(Mode, TextMode);

(function() {

    this.getNextLineIndent = function(state, line, tab) {
        var indent = this.$getIndent(line);

        if (state == "start") {
            var match = line.match(/^.*[\{\(\[]\s*$/);
            if (match) {
                indent += tab;
            }
        }

        return indent;
    };

    this.checkOutdent = function(state, line, input) {
        return this.$outdent.checkOutdent(line, input);
    };

    this.autoOutdent = function(state, doc, row) {
        this.$outdent.autoOutdent(doc, row);
    };

    this.createWorker = function(session) {
        var worker = new WorkerClient(["ace"], "ace/mode/json_worker", "JsonWorker");
        worker.attachToDocument(session.getDocument());

        worker.on("error", function(e) {
            session.setAnnotations([e.data]);
        });

        worker.on("ok", function() {
            session.clearAnnotations();
        });

        return worker;
    };


}).call(Mode.prototype);

exports.Mode = Mode;
});








