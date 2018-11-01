define(function(require, exports, module) {
    "use strict";

    var oop = require("../lib/oop");
    var TextMode = require("./text").Mode;
    var JsMode = require("./javascript").Mode;
    var RqlHighlightRules = require("./rql_highlight_rules").RqlHighlightRules;
    var WorkerClient = require("../worker/worker_client").WorkerClient;

    var Mode = function() {
        this.HighlightRules = RqlHighlightRules;
        this.$behaviour = this.$defaultBehaviour;

        this.createModeDelegates({
            "js-": JsMode
        });
        this.prefixRegexps = [/[a-zA-Z_0-9@'"\\\/\$\-\u00A2-\uFFFF=!<>]/];

        this.createWorker = function(session) {
            var worker = new WorkerClient(["ace"], "ace/mode/rql_worker", "RqlWorker");
            worker.attachToDocument(session.getDocument());

            worker.on("annotate", function(results) {
                session.setAnnotations(results.data);
            });

            worker.on("terminate", function() {
                session.clearAnnotations();
            });

            return worker;
        };
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
