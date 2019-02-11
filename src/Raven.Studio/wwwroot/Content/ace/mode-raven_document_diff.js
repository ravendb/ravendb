ace.define("ace/mode/raven_document_highlight_rules",["require","exports","module","ace/lib/lang","ace/lib/oop","ace/mode/text_highlight_rules"], function(require, exports, module) {
"use strict";

var lang = require("../lib/lang");
var oop = require("../lib/oop");
var TextHighlightRules = require("./text_highlight_rules").TextHighlightRules;

var RavenDocumentHighlightRules = function() {

    var startRules = [
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
    ];
    
    var stringRules = [
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
        }, {
            token : "string",
            regex : "",
            next  : "start"
        }
    ];
    
    var metadataStartRule = {
        token: "variable.metadata",
        regex: '"@metadata"',
        next:  function() {
            return "metadata-start";
        }
    };

    var curlyBracesCount = 0;
    this.$rules = {
        "start" : [metadataStartRule].concat(startRules),
        "string" : stringRules
    };
    
    this.embedRules({
        "start" : lang.deepCopy(startRules).map(function(rule) {
            rule.token = rule.token + ".metadata";
            return rule;
        }),
        "string" : lang.deepCopy(stringRules).map(function(rule) {
            rule.token = rule.token + ".metadata";
            return rule;
        })
    }, "metadata-", [ {
        token : function (value, currentState, stack) {
            curlyBracesCount++;
            return "paren.lparen.metadata";
        },
        regex: /{/
    }, {
        token : function (value, currentState, stack) {
            return "paren.rparen.metadata";
        },
        regex : /}/,
        next : function (currentState, stack) {
            if (--curlyBracesCount > 0) {
                return currentState;
            }
            return "start";
        }
    }]);
    
    this.normalizeRules();
};

oop.inherits(RavenDocumentHighlightRules, TextHighlightRules);

exports.RavenDocumentHighlightRules = RavenDocumentHighlightRules;
});

ace.define("ace/mode/matching_brace_outdent",["require","exports","module","ace/range"], function(require, exports, module) {
"use strict";

var Range = require("../range").Range;

var MatchingBraceOutdent = function() {};

(function() {

    this.checkOutdent = function(line, input) {
        if (! /^\s+$/.test(line))
            return false;

        return /^\s*\}/.test(input);
    };

    this.autoOutdent = function(doc, row) {
        var line = doc.getLine(row);
        var match = line.match(/^(\s*\})/);

        if (!match) return 0;

        var column = match[1].length;
        var openBracePos = doc.findMatchingBracket({row: row, column: column});

        if (!openBracePos || openBracePos.row == row) return 0;

        var indent = this.$getIndent(doc.getLine(openBracePos.row));
        doc.replace(new Range(row, 0, row, column-1), indent);
    };

    this.$getIndent = function(line) {
        return line.match(/^\s*/)[0];
    };

}).call(MatchingBraceOutdent.prototype);

exports.MatchingBraceOutdent = MatchingBraceOutdent;
});

ace.define("ace/mode/folding/raven_diff",["require","exports","module","ace/range"], function(require, exports, module) {
    "use strict";

    var Range = require("../../range").Range;

    var FoldMode = exports.FoldMode = function() {};

    (function() {
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

ace.define("ace/mode/raven_document_diff",["require","exports","module","ace/lib/oop","ace/mode/text","ace/mode/raven_document_highlight_rules","ace/mode/matching_brace_outdent","ace/mode/behaviour/cstyle","ace/mode/folding/raven_diff","ace/worker/worker_client"], function(require, exports, module) {
"use strict";

var oop = require("../lib/oop");
var TextMode = require("./text").Mode;
var HighlightRules = require("./raven_document_highlight_rules").RavenDocumentHighlightRules;
var MatchingBraceOutdent = require("./matching_brace_outdent").MatchingBraceOutdent;
var CstyleBehaviour = require("./behaviour/cstyle").CstyleBehaviour;
var RavenDiffFoldMode = require("./folding/raven_diff").FoldMode;
var WorkerClient = require("../worker/worker_client").WorkerClient;

var Mode = function() {
    this.HighlightRules = HighlightRules;
    this.$outdent = new MatchingBraceOutdent();
    this.$behaviour = new CstyleBehaviour();
    this.foldingRules = new RavenDiffFoldMode();
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

        worker.on("annotate", function(e) {
            session.setAnnotations(e.data);
        });

        worker.on("terminate", function() {
            session.clearAnnotations();
        });

        return worker;
    };

    this.$id = "ace/mode/raven_document_diff";
}).call(Mode.prototype);

exports.Mode = Mode;
});
