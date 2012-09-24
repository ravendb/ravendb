using System;

namespace Jint.Native {
    [Serializable]
    public class JsUndefined : JsObject {
        public static JsUndefined Instance = new JsUndefined() { Attributes = PropertyAttributes.DontEnum | PropertyAttributes.DontDelete };

        public JsUndefined() {
        }

        public override int Length {
            get {
                return 0;
            }
            set {
            }
        }

        public override bool IsClr
        {
            get
            {
                return false;
            }
        }

        public override Descriptor GetDescriptor(string index) {
            return null;
        }

        public override string Class {
            get { return CLASS_UNDEFINED; }
        }

        public override string Type
        {
            get
            {
                return TYPE_UNDEFINED;
            }
        }

        public override string ToString() {
            return "undefined";
        }

        public override object ToObject() {
            return null;
        }

        public override bool ToBoolean() {
            return false;
        }

        public override double ToNumber() {
            return double.NaN;
        }
    }
}
