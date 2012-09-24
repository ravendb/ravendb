using System;
using System.Globalization;

namespace Jint.Native {
    [Serializable]
    public sealed class JsDate : JsObject {
        static internal long OFFSET_1970 = new DateTime(1970, 1, 1, 0, 0, 0, DateTimeKind.Utc).Ticks;
        static internal int TICKSFACTOR = 10000;

        private new DateTime value;

        public override object Value {
            get {
                return value;
            }
            set {
                if (value is DateTime)
                    this.value = (DateTime)value;
                else if (value is double)
                    this.value = JsDateConstructor.CreateDateTime((double)value);
            }
        }

        public JsDate(JsObject prototype)
            : base(prototype) {
                value = new DateTime(1970, 1, 1, 0, 0, 0, DateTimeKind.Utc);
        }

        public JsDate(DateTime date, JsObject prototype): base(prototype) {
            value = date;
        }

        public JsDate(double value, JsObject prototype)
            : this(JsDateConstructor.CreateDateTime(value), prototype) {
        }

        public override bool IsClr
        {
            get
            {
                return false;
            }
        }

        public override double ToNumber() {
            return DateToDouble(value);
        }

        public static string FORMAT = "ddd, dd MMM yyyy HH':'mm':'ss 'GMT'zzz";
        public static string FORMATUTC = "ddd, dd MMM yyyy HH':'mm':'ss 'UTC'";
        public static string DATEFORMAT = "ddd, dd MMM yyyy";
        public static string TIMEFORMAT = "HH':'mm':'ss 'GMT'zzz";

        public static double DateToDouble(DateTime date) {
            return (date.ToUniversalTime().Ticks - OFFSET_1970) / TICKSFACTOR;
        }

        public static string DateToString(DateTime date) {
            return date.ToLocalTime().ToString(FORMAT, CultureInfo.InvariantCulture);
        }

        public override string ToString() {
            return DateToString(value);
        }

        public override object ToObject() {
            return value;
        }

        public override string Class {
            get { return CLASS_DATE; }
        }
    }
}
