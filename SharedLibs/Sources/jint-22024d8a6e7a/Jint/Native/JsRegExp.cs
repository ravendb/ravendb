using System;
using System.Text.RegularExpressions;

namespace Jint.Native {
    [Serializable]
    public class JsRegExp : JsObject {
        public bool IsGlobal { get { return this["global"].ToBoolean(); } }
        public bool IsIgnoreCase { get { return (options & RegexOptions.IgnoreCase) == RegexOptions.IgnoreCase; } }
        public bool IsMultiline { get { return (options & RegexOptions.Multiline) == RegexOptions.Multiline; } }

        private string pattern;
        private RegexOptions options;

        public JsRegExp(JsObject prototype)
            : base(prototype) {
        }

        public JsRegExp(string pattern, JsObject prototype)
            : this(pattern, false, false, false, prototype) {
        }

        public JsRegExp(string pattern, bool g, bool i, bool m, JsObject prototype)
            : base(prototype) {
            options = RegexOptions.ECMAScript;

            if (m) {
                options |= RegexOptions.Multiline;
            }

            if (i) {
                options |= RegexOptions.IgnoreCase;
            }

            this.pattern = pattern;
        }

        public string Pattern {
            get { return pattern; }
        }

        public Regex Regex {
            get { return new Regex(pattern, options); }
        }

        public RegexOptions Options {
            get { return options; }
        }

        public override bool IsClr
        {
            get
            {
                return false;
            }
        }

        public override object Value {
            get {
                return null;
            }
        }

        public override string ToSource() {
            return "/" + pattern.ToString() + "/";
        }

        public override string ToString() {
            return "/" + pattern.ToString() + "/" + (IsGlobal ? "g" : String.Empty) + (IsIgnoreCase ? "i" : String.Empty) + (IsMultiline ? "m" : String.Empty);
        }

        public override string Class {
            get { return CLASS_REGEXP; }
        }
    }
}
