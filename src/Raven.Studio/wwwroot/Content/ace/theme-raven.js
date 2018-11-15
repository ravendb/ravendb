ace.define("ace/theme/raven",["require","exports","module","ace/lib/dom"], function(require, exports, module) {

exports.isDark = true;
exports.cssClass = "ace-raven";
exports.cssText = "\
";

var dom = require("../lib/dom");
dom.importCssString(exports.cssText, exports.cssClass);
});
