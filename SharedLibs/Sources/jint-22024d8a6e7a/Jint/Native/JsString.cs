using System;

namespace Jint.Native {
    [Serializable]
    public sealed class JsString : JsLiteral {
        private new string value;

        public override object Value {
            get {
                return value;
            }
        }
        public JsString(JsObject prototype)
            : base(prototype) {
            value = String.Empty;
        }

        public JsString(string str, JsObject prototype)
            : base(prototype) {
            value = str;
        }

        public static bool StringToBoolean(string value) {
            if (value == null)
                return false;
            if (value == "true" || value.Length > 0) {
                return true;
            }

            return false;
        }

        public override bool IsClr
        {
            get
            {
                return false;
            }
        }

        public override bool ToBoolean() {
            return StringToBoolean(value);
        }

        public static double StringToNumber(string value) {
            if (value == null) {
                return double.NaN;
            }

            double result;

            if (Double.TryParse(value, System.Globalization.NumberStyles.AllowDecimalPoint, System.Globalization.CultureInfo.InvariantCulture, out result)) {
                return result;
            }
            else {
                return double.NaN;
            }
        }

        public override double ToNumber() {
            return StringToNumber(value);
        }

        public override string ToSource() {
            /// TODO: subsitute escape sequences
            return value == null ? "null" : "'" + ToString() + "'";
        }

        public override string ToString() {
            return value.ToString();
        }

        public override string Class {
            get { return CLASS_STRING; }
        }

        public override string Type
        {
            get
            {
                return TYPE_STRING;
            }
        }

        public override int GetHashCode() {
            return value.GetHashCode();
        }
    }
}
