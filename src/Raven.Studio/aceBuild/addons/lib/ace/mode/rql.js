define(function(require, exports, module) {
    "use strict";

    var oop = require("../lib/oop");
    var TextMode = require("./text").Mode;
    var JsMode = require("./javascript").Mode;
    var RqlHighlightRules = require("./rql_highlight_rules").RqlHighlightRules;

    var Mode = function() {
        this.HighlightRules = RqlHighlightRules;
        this.$behaviour = this.$defaultBehaviour;

        this.createModeDelegates({
            "js-": JsMode
        });
        this.prefixRegexps = [/[a-zA-Z_0-9@'"\\\/\$\-\u00A2-\uFFFF=!<>]/];
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
