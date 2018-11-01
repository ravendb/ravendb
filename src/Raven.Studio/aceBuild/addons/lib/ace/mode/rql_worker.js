
define(function(require, exports, module) {
    "use strict";

    var oop = require("../lib/oop");
    var Mirror = require("../worker/mirror").Mirror;
    var lint = require("./javascript/jshint").JSHINT;

    function startRegex(arr) {
        return RegExp("^(" + arr.join("|") + ")");
    }

    var disabledWarningsRe = startRegex([
        "Bad for in variable '(.+)'.",
        'Missing "use strict"'
    ]);
    var errorsRe = startRegex([
        "Unexpected",
        "Expected ",
        "Confusing (plus|minus)",
        "\\{a\\} unterminated regular expression",
        "Unclosed ",
        "Unmatched ",
        "Unbegun comment",
        "Bad invocation",
        "Missing space after",
        "Missing operator at"
    ]);
    var infoRe = startRegex([
        "Expected an assignment",
        "Bad escapement of EOL",
        "Unexpected comma",
        "Unexpected space",
        "Missing radix parameter.",
        "A leading decimal point can",
        "\\['{a}'\\] is better written in dot notation.",
        "'{a}' used out of scope"
    ]);

    var RqlWorker = exports.RqlWorker = function(sender) {
        Mirror.call(this, sender);
        this.setTimeout(500);
        this.setOptions();
    };

    oop.inherits(RqlWorker, Mirror);

    (function() {
        this.setOptions = function(options) {
            this.options = options || {
                esnext: true,
                moz: true,
                devel: true,
                browser: true,
                node: true,
                laxcomma: true,
                laxbreak: true,
                lastsemic: true,
                onevar: false,
                passfail: false,
                maxerr: 100,
                expr: true,
                multistr: true,
                globalstrict: true
            };
            this.doc.getValue() && this.deferredUpdate.schedule(100);
        };

        this.changeOptions = function(newOptions) {
            oop.mixin(this.options, newOptions);
            this.doc.getValue() && this.deferredUpdate.schedule(100);
        };

        this.isValidJS = function(str) {
            try {
                eval("throw 0;" + str);
            } catch(e) {
                if (e === 0)
                    return true;
            }
            return false
        };

        this.onUpdate = function() {
            var value = this.doc.getValue();

            value = value.replace(/^#!.*\n/, "\n");
            if (!value)
                return this.sender.emit("annotate", []);

            var errors = [];

            var result;
            while (result = declareFunctionRegex.exec(value)) {
                var start = result.index + result["0"].length;
                var end = getEndOfDeclareFunctino(value,  start);
                if (end > 0) {
                    var script = value.substring(start, end);
                    var lastNewLineIndex = value.lastIndexOf("\n", start);
                    script =  new Array( start - lastNewLineIndex ).join( " " ) + script;
                    lastNewLineIndex = value.lastIndexOf("\n", lastNewLineIndex - 1);
                    while (lastNewLineIndex > -1){
                        script = "\n" + script;
                        lastNewLineIndex = value.lastIndexOf("\n", lastNewLineIndex - 1);
                    }
                    validateFunction.apply(this, [script, errors]);
                }
            }

            this.sender.emit("annotate", errors);
        };

        var declareFunctionRegex = /declare\s+function\s+[^{]*{/g;

        var getEndOfDeclareFunctino = function (value, start) {
            var curelyBracesCount = 0;
            var inDoubleQoutes = false;
            var inSingleQoutes = false;
            for (var i = start; i < value.length; i++) {
                var c = value[i];
                switch (c) {
                    case "{":
                        if (!inSingleQoutes && !inDoubleQoutes) {
                            curelyBracesCount++;
                        }
                        break;
                    case "}":
                        if (!inSingleQoutes && !inDoubleQoutes) {
                            if (curelyBracesCount === 0) {
                                return i;
                            }
                            curelyBracesCount--;
                        }
                        break;
                    case "'":
                        if (!inDoubleQoutes) {
                            inSingleQoutes = !inSingleQoutes;
                        }
                        break;
                    case '"':
                        if (!inSingleQoutes) {
                            inDoubleQoutes = !inDoubleQoutes;
                        }
                        break;
                    case "\\":
                        i++;
                        break;
                    case "\n":
                        inDoubleQoutes = inSingleQoutes = false;
                        break;
                }
            }

            return -1;
        };

        var validateFunction = function (value, errors) {
            var maxErrorLevel = this.isValidJS(value) ? "warning" : "error";
            lint(value, this.options, this.options.globals);
            var results = lint.errors;

            var errorAdded = false
            for (var i = 0; i < results.length; i++) {
                var error = results[i];
                if (!error)
                    continue;
                var raw = error.raw;
                var type = "warning";

                if (raw == "Missing semicolon.") {
                    var str = error.evidence.substr(error.character);
                    str = str.charAt(str.search(/\S/));
                    if (maxErrorLevel == "error" && str && /[\w\d{(['"]/.test(str)) {
                        error.reason = 'Missing ";" before statement';
                        type = "error";
                    } else {
                        type = "info";
                    }
                }
                else if (disabledWarningsRe.test(raw)) {
                    continue;
                }
                else if (infoRe.test(raw)) {
                    type = "info"
                }
                else if (errorsRe.test(raw)) {
                    errorAdded  = true;
                    type = maxErrorLevel;
                }
                else if (raw == "'{a}' is not defined.") {
                    type = "warning";
                }
                else if (raw == "'{a}' is defined but never used.") {
                    type = "info";
                }

                errors.push({
                    row: error.line-1,
                    column: error.character-1,
                    text: error.reason,
                    type: type,
                    raw: raw
                });

                if (errorAdded) {
                }
            }
        };

    }).call(RqlWorker.prototype);
});
