using System;
using System.Collections.Generic;
using System.Text;
using System.Globalization;

namespace Jint.Native {
    [Serializable]
    public sealed class JsNumber : JsLiteral {
        private new double value;

        public override object Value {
            get {
                return value;
            }
        }

        public JsNumber(JsObject prototype)
            : this(0d, prototype) {
        }

        public JsNumber(double num, JsObject prototype)
            : base(prototype) {
            value = num;
        }

        public JsNumber(int num, JsObject prototype)
            : base(prototype) {
            value = num;
        }

        public override bool IsClr
        {
            get
            {
                return false;
            }
        }

        public static bool NumberToBoolean(double value) {
            return value != 0 && !Double.IsNaN(value);
        }

        public override bool ToBoolean() {
            return NumberToBoolean(value);
        }

        public override double ToNumber() {
            return value;
        }

        public override string ToString() {
            return value.ToString(CultureInfo.InvariantCulture);
        }

        public override object ToObject() {
            return value;
        }

        public override string Class {
            get { return CLASS_NUMBER; }
        }

        public override string Type
        {
            get
            {
                return TYPE_NUMBER;
            }
        }
    }
}
