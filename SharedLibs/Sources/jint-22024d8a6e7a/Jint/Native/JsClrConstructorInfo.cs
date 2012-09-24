using System;
using System.Collections.Generic;
using System.Text;
using System.Text.RegularExpressions;
using Jint.Delegates;
using System.Reflection;

namespace Jint.Native {
    [Serializable]
    public class JsClrConstructorInfo : JsObject {
        private ConstructorInfo value;

        public JsClrConstructorInfo() {
            value = null;
        }

        public JsClrConstructorInfo(ConstructorInfo clr) {
            value = clr;
        }

        public override bool ToBoolean() {
            return false;
        }

        public override double ToNumber() {
            return 0;
        }

        public override string ToString() {
            return value.Name;
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
