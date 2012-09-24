using System;
using System.Collections.Generic;
using System.Text;
using System.Text.RegularExpressions;

namespace Jint.Native {
    [Serializable]
    public class JsClrMethodInfo : JsObject {
        private new string value;

        public JsClrMethodInfo() {
        }

        public JsClrMethodInfo(string method) {
            value = method;
        }

        public override bool ToBoolean() {
            return false;
        }

        public override double ToNumber() {
            return 0;
        }

        public override string ToString() {
            return String.Empty;
        }

        public const string TYPEOF = "clrMethodInfo";

        public override string Class {
            get { return TYPEOF; }
        }

        public override object Value {
            get {
                return value;
            }
        }
    }
}
