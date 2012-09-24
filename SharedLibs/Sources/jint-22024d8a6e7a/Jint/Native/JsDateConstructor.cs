using System;
using System.Collections.Generic;
using System.Text;
using System.Globalization;
using System.Text.RegularExpressions;
using Jint.Expressions;

namespace Jint.Native {
    [Serializable]
    public class JsDateConstructor : JsConstructor {
        protected JsDateConstructor(IGlobal global, bool initializeUTC)
            : base(global) {
            Name = "Date";
            DefineOwnProperty(PROTOTYPE, global.ObjectClass.New(this), PropertyAttributes.DontEnum | PropertyAttributes.DontDelete | PropertyAttributes.ReadOnly);

            DefineOwnProperty("now", new ClrFunction(new Func<JsDate>(() => { return Global.DateClass.New(DateTime.Now); }), global.FunctionClass.PrototypeProperty), PropertyAttributes.DontEnum);
            DefineOwnProperty("parse", new JsFunctionWrapper(ParseImpl, global.FunctionClass.PrototypeProperty), PropertyAttributes.DontEnum);
            DefineOwnProperty("parseLocale", new JsFunctionWrapper(ParseLocaleImpl, global.FunctionClass.PrototypeProperty), PropertyAttributes.DontEnum);
            DefineOwnProperty("UTC", new JsFunctionWrapper(UTCImpl, global.FunctionClass.PrototypeProperty), PropertyAttributes.DontEnum);
        }

        public override void InitPrototype(IGlobal global) {
            //Prototype = global.FunctionClass;
            var Prototype = PrototypeProperty;

            Prototype.DefineOwnProperty("UTC", new JsFunctionWrapper(UTCImpl, global.FunctionClass.PrototypeProperty), PropertyAttributes.DontEnum);

            #region Static Methods
            Prototype.DefineOwnProperty("now", new ClrFunction(new Func<JsDate>(() => { return Global.DateClass.New(DateTime.Now); }), global.FunctionClass.PrototypeProperty), PropertyAttributes.DontEnum);
            Prototype.DefineOwnProperty("parse", new JsFunctionWrapper(ParseImpl, global.FunctionClass.PrototypeProperty), PropertyAttributes.DontEnum);
            Prototype.DefineOwnProperty("parseLocale", new JsFunctionWrapper(ParseLocaleImpl, global.FunctionClass.PrototypeProperty), PropertyAttributes.DontEnum);
            #endregion

            #region Methods
            Prototype.DefineOwnProperty("toString", global.FunctionClass.New<JsDictionaryObject>(ToStringImpl), PropertyAttributes.DontEnum);
            Prototype.DefineOwnProperty("toDateString", global.FunctionClass.New<JsDictionaryObject>(ToDateStringImpl, 0), PropertyAttributes.DontEnum);
            Prototype.DefineOwnProperty("toTimeString", global.FunctionClass.New<JsDictionaryObject>(ToTimeStringImpl, 0), PropertyAttributes.DontEnum);
            Prototype.DefineOwnProperty("toLocaleString", global.FunctionClass.New<JsDictionaryObject>(ToLocaleStringImpl), PropertyAttributes.DontEnum);
            Prototype.DefineOwnProperty("toLocaleDateString", global.FunctionClass.New<JsDictionaryObject>(ToLocaleDateStringImpl), PropertyAttributes.DontEnum);
            Prototype.DefineOwnProperty("toLocaleTimeString", global.FunctionClass.New<JsDictionaryObject>(ToLocaleTimeStringImpl), PropertyAttributes.DontEnum);
            Prototype.DefineOwnProperty("valueOf", global.FunctionClass.New<JsDictionaryObject>(ValueOfImpl), PropertyAttributes.DontEnum);
            Prototype.DefineOwnProperty("getTime", global.FunctionClass.New<JsDictionaryObject>(GetTimeImpl), PropertyAttributes.DontEnum);
            Prototype.DefineOwnProperty("getFullYear", global.FunctionClass.New<JsDictionaryObject>(GetFullYearImpl), PropertyAttributes.DontEnum);
            Prototype.DefineOwnProperty("getUTCFullYear", global.FunctionClass.New<JsDictionaryObject>(GetUTCFullYearImpl), PropertyAttributes.DontEnum);
            Prototype.DefineOwnProperty("getMonth", global.FunctionClass.New<JsDictionaryObject>(GetMonthImpl), PropertyAttributes.DontEnum);
            Prototype.DefineOwnProperty("getUTCMonth", global.FunctionClass.New<JsDictionaryObject>(GetUTCMonthImpl), PropertyAttributes.DontEnum);
            Prototype.DefineOwnProperty("getDate", global.FunctionClass.New<JsDictionaryObject>(GetDateImpl), PropertyAttributes.DontEnum);
            Prototype.DefineOwnProperty("getUTCDate", global.FunctionClass.New<JsDictionaryObject>(GetUTCDateImpl), PropertyAttributes.DontEnum);
            Prototype.DefineOwnProperty("getDay", global.FunctionClass.New<JsDictionaryObject>(GetDayImpl), PropertyAttributes.DontEnum);
            Prototype.DefineOwnProperty("getUTCDay", global.FunctionClass.New<JsDictionaryObject>(GetUTCDayImpl), PropertyAttributes.DontEnum);
            Prototype.DefineOwnProperty("getHours", global.FunctionClass.New<JsDictionaryObject>(GetHoursImpl), PropertyAttributes.DontEnum);
            Prototype.DefineOwnProperty("getUTCHours", global.FunctionClass.New<JsDictionaryObject>(GetUTCHoursImpl), PropertyAttributes.DontEnum);
            Prototype.DefineOwnProperty("getMinutes", global.FunctionClass.New<JsDictionaryObject>(GetMinutesImpl), PropertyAttributes.DontEnum);
            Prototype.DefineOwnProperty("getUTCMinutes", global.FunctionClass.New<JsDictionaryObject>(GetUTCMinutesImpl), PropertyAttributes.DontEnum);
            Prototype.DefineOwnProperty("getSeconds", global.FunctionClass.New<JsDictionaryObject>(GetSecondsImpl), PropertyAttributes.DontEnum);
            Prototype.DefineOwnProperty("getUTCSeconds", global.FunctionClass.New<JsDictionaryObject>(GetUTCSecondsImpl), PropertyAttributes.DontEnum);
            Prototype.DefineOwnProperty("getMilliseconds", global.FunctionClass.New<JsDictionaryObject>(GetMillisecondsImpl), PropertyAttributes.DontEnum);
            Prototype.DefineOwnProperty("getUTCMilliseconds", global.FunctionClass.New<JsDictionaryObject>(GetUTCMillisecondsImpl), PropertyAttributes.DontEnum);
            Prototype.DefineOwnProperty("getTimezoneOffset", global.FunctionClass.New<JsDictionaryObject>(GetTimezoneOffsetImpl), PropertyAttributes.DontEnum);

            Prototype.DefineOwnProperty("setTime", global.FunctionClass.New<JsDictionaryObject>(SetTimeImpl), PropertyAttributes.DontEnum);
            Prototype.DefineOwnProperty("setMilliseconds", global.FunctionClass.New<JsDictionaryObject>(SetMillisecondsImpl), PropertyAttributes.DontEnum);
            Prototype.DefineOwnProperty("setUTCMilliseconds", global.FunctionClass.New<JsDictionaryObject>(SetUTCMillisecondsImpl), PropertyAttributes.DontEnum);
            Prototype.DefineOwnProperty("setSeconds", global.FunctionClass.New<JsDictionaryObject>(SetSecondsImpl, 2), PropertyAttributes.DontEnum);
            Prototype.DefineOwnProperty("setUTCSeconds", global.FunctionClass.New<JsDictionaryObject>(SetUTCSecondsImpl, 2), PropertyAttributes.DontEnum);
            Prototype.DefineOwnProperty("setMinutes", global.FunctionClass.New<JsDictionaryObject>(SetMinutesImpl, 3), PropertyAttributes.DontEnum);
            Prototype.DefineOwnProperty("setUTCMinutes", global.FunctionClass.New<JsDictionaryObject>(SetUTCMinutesImpl, 3), PropertyAttributes.DontEnum);
            Prototype.DefineOwnProperty("setHours", global.FunctionClass.New<JsDictionaryObject>(SetHoursImpl, 4), PropertyAttributes.DontEnum);
            Prototype.DefineOwnProperty("setUTCHours", global.FunctionClass.New<JsDictionaryObject>(SetUTCHoursImpl, 4), PropertyAttributes.DontEnum);
            Prototype.DefineOwnProperty("setDate", global.FunctionClass.New<JsDictionaryObject>(SetDateImpl), PropertyAttributes.DontEnum);
            Prototype.DefineOwnProperty("setUTCDate", global.FunctionClass.New<JsDictionaryObject>(SetUTCDateImpl), PropertyAttributes.DontEnum);
            Prototype.DefineOwnProperty("setMonth", global.FunctionClass.New<JsDictionaryObject>(SetMonthImpl, 2), PropertyAttributes.DontEnum);
            Prototype.DefineOwnProperty("setUTCMonth", global.FunctionClass.New<JsDictionaryObject>(SetUTCMonthImpl, 2), PropertyAttributes.DontEnum);
            Prototype.DefineOwnProperty("setFullYear", global.FunctionClass.New<JsDictionaryObject>(SetFullYearImpl, 3), PropertyAttributes.DontEnum);
            Prototype.DefineOwnProperty("setUTCFullYear", global.FunctionClass.New<JsDictionaryObject>(SetUTCFullYearImpl, 3), PropertyAttributes.DontEnum);

            Prototype.DefineOwnProperty("toUTCString", global.FunctionClass.New<JsDictionaryObject>(ToUTCStringImpl), PropertyAttributes.DontEnum);
            #endregion
        }
        public JsDateConstructor(IGlobal global)
            : this(global, true) {
        }

        public JsDate New() {
            return new JsDate(this.PrototypeProperty);
        }

        public JsDate New(double value) {
            return new JsDate(value, this.PrototypeProperty);
        }

        public JsDate New(DateTime value) {
            return new JsDate(value, this.PrototypeProperty);
        }

        public JsDate New(DateTime value, JsObject prototype)
        {
            return new JsDate(value, prototype);
        }

        public JsDate Construct(JsInstance[] parameters) {
            JsDate result = null;

            if (parameters.Length == 1) {
                if ((parameters[0].Class == JsInstance.CLASS_NUMBER || parameters[0].Class == JsInstance.CLASS_OBJECT) && double.IsNaN(parameters[0].ToNumber())) {
                    result = New(double.NaN);
                }
                else if (parameters[0].Class == JsInstance.CLASS_NUMBER)
                {
                    result = New(parameters[0].ToNumber());
                }
                else {
                    double d;
                    if (ParseDate(parameters[0].ToString(), CultureInfo.InvariantCulture, out d)) {
                        result = New(d);
                    }
                    else if (ParseDate(parameters[0].ToString(), CultureInfo.CurrentCulture, out d)) {
                        result = New(d);
                    }
                    else {
                        result = New(0);
                    }
                }
            }
            else if (parameters.Length > 1) {
                DateTime d = new DateTime(0, DateTimeKind.Local);

                if (parameters.Length > 0) {
                    int year = (int)parameters[0].ToNumber() - 1;
                    if (year < 100) {
                        year += 1900;
                    }

                    d = d.AddYears(year);
                }

                if (parameters.Length > 1) {
                    d = d.AddMonths((int)parameters[1].ToNumber());
                }

                if (parameters.Length > 2) {
                    d = d.AddDays((int)parameters[2].ToNumber() - 1);
                }

                if (parameters.Length > 3) {
                    d = d.AddHours((int)parameters[3].ToNumber());
                }

                if (parameters.Length > 4) {
                    d = d.AddMinutes((int)parameters[4].ToNumber());
                }

                if (parameters.Length > 5) {
                    d = d.AddSeconds((int)parameters[5].ToNumber());
                }

                if (parameters.Length > 6) {
                    d = d.AddMilliseconds((int)parameters[6].ToNumber());
                }

                result = New(d);
            }
            else {
                result = New(DateTime.UtcNow);
            }

            return result;
        }

        public override JsInstance Execute(IJintVisitor visitor, JsDictionaryObject that, JsInstance[] parameters) {
            JsDate result = Construct(parameters);

            if (that == null || (that as IGlobal) == visitor.Global)
            {
                return visitor.Return(ToStringImpl(result, JsInstance.EMPTY));
            }

            return result;
        }

        private bool ParseDate(string p, IFormatProvider culture, out double result) {
            DateTime d = new DateTime(0, DateTimeKind.Utc); ;
            result = 0;

            if (DateTime.TryParse(p, culture, DateTimeStyles.None, out d)) {
                result = New(d).ToNumber();
                return true;
            }

            if (DateTime.TryParseExact(p, JsDate.FORMAT, culture, DateTimeStyles.None, out d)) {
                result = New(d).ToNumber();
                return true;
            }

            DateTime ld;

            if (DateTime.TryParseExact(p, JsDate.DATEFORMAT, culture, DateTimeStyles.None, out ld)) {
                d = d.AddTicks(ld.Ticks);
            }

            if (DateTime.TryParseExact(p, JsDate.TIMEFORMAT, culture, DateTimeStyles.None, out ld)) {
                d = d.AddTicks(ld.Ticks);
            }

            if (d.Ticks > 0) {
                result = New(d).ToNumber();
                return true;
            }

            return true;
        }

        /// <summary>
        /// 15.9.4.1
        /// </summary>
        /// <param name="str"></param>
        /// <returns></returns>
        public JsInstance ParseImpl(JsInstance[] parameters) {
            double d;
            if (ParseDate(parameters[0].ToString(), CultureInfo.InvariantCulture, out d)) {
                return Global.NumberClass.New(d);
            }
            else {
                return Global.NaN;
            }
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="str"></param>
        /// <returns></returns>
        public JsInstance ParseLocaleImpl(JsInstance[] parameters) {
            double d;
            if (ParseDate(parameters[0].ToString(), CultureInfo.CurrentCulture, out d)) {
                return Global.NumberClass.New(d);
            }
            else {
                return Global.NaN;
            }
        }

        internal static DateTime CreateDateTime(double number) {
            return new DateTime((long)(number * JsDate.TICKSFACTOR + JsDate.OFFSET_1970), DateTimeKind.Utc);
        }

        /// <summary>
        /// 15.9.5.2
        /// </summary>
        /// <param name="target"></param>
        /// <param name="parameters"></param>
        /// <returns></returns>
        public JsInstance ToStringImpl(JsDictionaryObject target, JsInstance[] parameters) {
            if (double.IsNaN(target.ToNumber())) {
                return Global.StringClass.New(double.NaN.ToString());
            }

            return Global.StringClass.New(CreateDateTime(target.ToNumber()).ToLocalTime().ToString(JsDate.FORMAT, CultureInfo.InvariantCulture));
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="target"></param>
        /// <param name="parameters"></param>
        /// <returns></returns>
        public JsInstance ToLocaleStringImpl(JsDictionaryObject target, JsInstance[] parameters) {
            if (double.IsNaN(target.ToNumber())) {
                return Global.StringClass.New(double.NaN.ToString());
            }

            return Global.StringClass.New(CreateDateTime(target.ToNumber()).ToLocalTime().ToString("F", CultureInfo.CurrentCulture));
        }

        /// <summary>
        /// 15.9.5.3
        /// </summary>
        /// <param name="target"></param>
        /// <param name="parameters"></param>
        /// <returns></returns>
        public JsInstance ToDateStringImpl(JsDictionaryObject target, JsInstance[] parameters) {
            if (double.IsNaN(target.ToNumber())) {
                return Global.StringClass.New(double.NaN.ToString());
            }

            return Global.StringClass.New(CreateDateTime(target.ToNumber()).ToLocalTime().ToString(JsDate.DATEFORMAT, CultureInfo.InvariantCulture));
        }

        /// <summary>
        /// 15.9.5.4
        /// </summary>
        /// <param name="target"></param>
        /// <param name="parameters"></param>
        /// <returns></returns>
        public JsInstance ToTimeStringImpl(JsDictionaryObject target, JsInstance[] parameters) {
            if (double.IsNaN(target.ToNumber())) {
                return Global.StringClass.New(double.NaN.ToString());
            }

            return Global.StringClass.New(CreateDateTime(target.ToNumber()).ToLocalTime().ToString(JsDate.TIMEFORMAT, CultureInfo.InvariantCulture));
        }

        /// <summary>
        /// 15.9.5.6
        /// </summary>
        /// <param name="target"></param>
        /// <param name="parameters"></param>
        /// <returns></returns>
        public JsInstance ToLocaleDateStringImpl(JsDictionaryObject target, JsInstance[] parameters) {
            if (double.IsNaN(target.ToNumber())) {
                return Global.StringClass.New(double.NaN.ToString());
            }

            return Global.StringClass.New(CreateDateTime(target.ToNumber()).ToLocalTime().ToString(JsDate.DATEFORMAT));
        }

        /// <summary>
        /// 15.9.5.7
        /// </summary>
        /// <param name="target"></param>
        /// <param name="parameters"></param>
        /// <returns></returns>
        public JsInstance ToLocaleTimeStringImpl(JsDictionaryObject target, JsInstance[] parameters) {
            if (double.IsNaN(target.ToNumber())) {
                return Global.StringClass.New(double.NaN.ToString());
            }

            return Global.StringClass.New(CreateDateTime(target.ToNumber()).ToLocalTime().ToString(JsDate.TIMEFORMAT));
        }

        /// <summary>
        /// 15.9.5.8
        /// </summary>
        /// <param name="target"></param>
        /// <param name="parameters"></param>
        /// <returns></returns>
        public JsInstance ValueOfImpl(JsDictionaryObject target, JsInstance[] parameters) {
            if (double.IsNaN(target.ToNumber())) {
                return Global.NaN;
            }

            return Global.NumberClass.New(target.ToNumber());
        }

        /// <summary>
        /// 15.9.5.9
        /// </summary>
        /// <param name="target"></param>
        /// <param name="parameters"></param>
        /// <returns></returns>
        public JsInstance GetTimeImpl(JsDictionaryObject target, JsInstance[] parameters) {
            if (double.IsNaN(target.ToNumber())) {
                return Global.NaN;
            }

            return Global.NumberClass.New(target.ToNumber());
        }

        /// <summary>
        /// 15.9.5.10
        /// </summary>
        /// <param name="target"></param>
        /// <param name="parameters"></param>
        /// <returns></returns>
        public JsInstance GetFullYearImpl(JsDictionaryObject target, JsInstance[] parameters) {
            if (double.IsNaN(target.ToNumber())) {
                return Global.NaN;
            }

            return Global.NumberClass.New(CreateDateTime(target.ToNumber()).ToLocalTime().Year);
        }

        /// <summary>
        /// 15.9.5.11
        /// </summary>
        /// <param name="target"></param>
        /// <param name="parameters"></param>
        /// <returns></returns>
        public JsInstance GetUTCFullYearImpl(JsDictionaryObject target, JsInstance[] parameters) {
            if (double.IsNaN(target.ToNumber())) {
                return Global.NaN;
            }

            return Global.NumberClass.New(CreateDateTime(target.ToNumber()).ToUniversalTime().Year);
        }

        /// <summary>
        /// 15.9.5.12
        /// </summary>
        /// <param name="target"></param>
        /// <param name="parameters"></param>
        /// <returns></returns>
        public JsInstance GetMonthImpl(JsDictionaryObject target, JsInstance[] parameters) {
            if (double.IsNaN(target.ToNumber())) {
                return Global.NaN;
            }

            return Global.NumberClass.New(CreateDateTime(target.ToNumber()).ToLocalTime().Month - 1);
        }

        /// <summary>
        /// 15.9.5.13
        /// </summary>
        /// <param name="target"></param>
        /// <param name="parameters"></param>
        /// <returns></returns>
        public JsInstance GetUTCMonthImpl(JsDictionaryObject target, JsInstance[] parameters) {
            if (double.IsNaN(target.ToNumber())) {
                return Global.NaN;
            }

            return Global.NumberClass.New(CreateDateTime(target.ToNumber()).ToUniversalTime().Month - 1);

        }

        /// <summary>
        /// 15.9.5.14
        /// </summary>
        /// <param name="target"></param>
        /// <param name="parameters"></param>
        /// <returns></returns>
        public JsInstance GetDateImpl(JsDictionaryObject target, JsInstance[] parameters) {
            if (double.IsNaN(target.ToNumber())) {
                return Global.NaN;
            }

            return Global.NumberClass.New(CreateDateTime(target.ToNumber()).ToLocalTime().Day);
        }

        /// <summary>
        /// 15.9.5.15
        /// </summary>
        /// <param name="target"></param>
        /// <param name="parameters"></param>
        /// <returns></returns>
        public JsInstance GetUTCDateImpl(JsDictionaryObject target, JsInstance[] parameters) {
            if (double.IsNaN(target.ToNumber())) {
                return Global.NaN;
            }

            return Global.NumberClass.New(CreateDateTime(target.ToNumber()).ToUniversalTime().Day);
        }

        /// <summary>
        /// 15.9.5.16
        /// </summary>
        /// <param name="target"></param>
        /// <param name="parameters"></param>
        /// <returns></returns>
        public JsInstance GetDayImpl(JsDictionaryObject target, JsInstance[] parameters) {
            if (double.IsNaN(target.ToNumber())) {
                return Global.NaN;
            }

            return Global.NumberClass.New((int)CreateDateTime(target.ToNumber()).ToLocalTime().DayOfWeek);
        }

        /// <summary>
        /// 15.9.5.17
        /// </summary>
        /// <param name="target"></param>
        /// <param name="parameters"></param>
        /// <returns></returns>
        public JsInstance GetUTCDayImpl(JsDictionaryObject target, JsInstance[] parameters) {
            if (double.IsNaN(target.ToNumber())) {
                return Global.NaN;
            }

            return Global.NumberClass.New((int)CreateDateTime(target.ToNumber()).ToUniversalTime().DayOfWeek);
        }

        /// <summary>
        /// 15.9.5.18
        /// </summary>
        /// <param name="target"></param>
        /// <param name="parameters"></param>
        /// <returns></returns>
        public JsInstance GetHoursImpl(JsDictionaryObject target, JsInstance[] parameters) {
            if (double.IsNaN(target.ToNumber())) {
                return Global.NaN;
            }

            return Global.NumberClass.New(CreateDateTime(target.ToNumber()).ToLocalTime().Hour);
        }

        /// <summary>
        /// 15.9.5.19
        /// </summary>
        /// <param name="target"></param>
        /// <param name="parameters"></param>
        /// <returns></returns>
        public JsInstance GetUTCHoursImpl(JsDictionaryObject target, JsInstance[] parameters) {
            if (double.IsNaN(target.ToNumber())) {
                return Global.NaN;
            }

            return Global.NumberClass.New(CreateDateTime(target.ToNumber()).ToUniversalTime().Hour);
        }

        /// <summary>
        /// 15.9.5.20
        /// </summary>
        /// <param name="target"></param>
        /// <param name="parameters"></param>
        /// <returns></returns>
        public JsInstance GetMinutesImpl(JsDictionaryObject target, JsInstance[] parameters) {
            if (double.IsNaN(target.ToNumber())) {
                return Global.NaN;
            }

            return Global.NumberClass.New(CreateDateTime(target.ToNumber()).ToLocalTime().Minute);
        }

        /// <summary>
        /// 15.9.5.21
        /// </summary>
        /// <param name="target"></param>
        /// <param name="parameters"></param>
        /// <returns></returns>
        public JsInstance GetUTCMinutesImpl(JsDictionaryObject target, JsInstance[] parameters) {
            if (double.IsNaN(target.ToNumber())) {
                return Global.NaN;
            }

            return Global.NumberClass.New(CreateDateTime(target.ToNumber()).ToUniversalTime().Minute);
        }

        /// <summary>
        /// 15.9.5.
        /// </summary>
        /// <param name="target"></param>
        /// <param name="parameters"></param>
        /// <returns></returns>
        public JsInstance ToUTCStringImpl(JsDictionaryObject target, JsInstance[] parameters) {
            if (double.IsNaN(target.ToNumber())) {
                return Global.StringClass.New(double.NaN.ToString());
            }

            return Global.StringClass.New(CreateDateTime(target.ToNumber()).ToString(JsDate.FORMATUTC, CultureInfo.InvariantCulture));
        }

        /// <summary>
        /// 15.9.5.22
        /// </summary>
        /// <param name="target"></param>
        /// <param name="parameters"></param>
        /// <returns></returns>
        public JsInstance GetSecondsImpl(JsDictionaryObject target, JsInstance[] parameters) {
            if (double.IsNaN(target.ToNumber())) {
                return Global.NaN;
            }

            return Global.NumberClass.New(CreateDateTime(target.ToNumber()).ToLocalTime().Second);
        }

        /// <summary>
        /// 15.9.5.23
        /// </summary>
        /// <param name="target"></param>
        /// <param name="parameters"></param>
        /// <returns></returns>
        public JsInstance GetUTCSecondsImpl(JsDictionaryObject target, JsInstance[] parameters) {
            if (double.IsNaN(target.ToNumber())) {
                return Global.NaN;
            }

            return Global.NumberClass.New(CreateDateTime(target.ToNumber()).ToUniversalTime().Second);
        }

        /// <summary>
        /// 15.9.5.24
        /// </summary>
        /// <param name="target"></param>
        /// <param name="parameters"></param>
        /// <returns></returns>
        public JsInstance GetMillisecondsImpl(JsDictionaryObject target, JsInstance[] parameters) {
            if (double.IsNaN(target.ToNumber())) {
                return Global.NaN;
            }

            return Global.NumberClass.New(CreateDateTime(target.ToNumber()).ToLocalTime().Millisecond);
        }

        /// <summary>
        /// 15.9.5.25
        /// </summary>
        /// <param name="target"></param>
        /// <param name="parameters"></param>
        /// <returns></returns>
        public JsInstance GetUTCMillisecondsImpl(JsDictionaryObject target, JsInstance[] parameters) {
            if (double.IsNaN(target.ToNumber())) {
                return Global.NaN;
            }

            return Global.NumberClass.New(CreateDateTime(target.ToNumber()).ToUniversalTime().Millisecond);
        }

        /// <summary>
        /// 15.9.5.26
        /// </summary>
        /// <param name="target"></param>
        /// <param name="parameters"></param>
        /// <returns></returns>
        public JsInstance GetTimezoneOffsetImpl(JsDictionaryObject target, JsInstance[] parameters) {
            return Global.NumberClass.New(-TimeZone.CurrentTimeZone.GetUtcOffset(new DateTime()).TotalMinutes);
        }

        /// <summary>
        /// 15.9.5.27
        /// </summary>
        /// <param name="target"></param>
        /// <param name="parameters"></param>
        /// <returns></returns>
        public JsInstance SetTimeImpl(JsDictionaryObject target, JsInstance[] parameters) {
            if (parameters.Length == 0)
                throw new ArgumentException("There was no tiem specified");

            target.Value = parameters[0].ToNumber();
            return target;
        }

        /// <summary>
        /// 15.9.5.28
        /// </summary>
        /// <param name="target"></param>
        /// <param name="parameters"></param>
        /// <returns></returns>
        public JsInstance SetMillisecondsImpl(JsDictionaryObject target, JsInstance[] parameters) {
            if (parameters.Length == 0)
                throw new ArgumentException("There was no millisecond specified");

            DateTime valueOf = CreateDateTime(target.ToNumber()).ToLocalTime();
            valueOf = valueOf.AddMilliseconds(-valueOf.Millisecond);
            valueOf = valueOf.AddMilliseconds(parameters[0].ToNumber());
            target.Value = New(valueOf).ToNumber();
            return target;
        }

        /// <summary>
        /// 15.9.5.29
        /// </summary>
        /// <param name="target"></param>
        /// <param name="parameters"></param>
        /// <returns></returns>
        public JsInstance SetUTCMillisecondsImpl(JsDictionaryObject target, JsInstance[] parameters) {
            if (parameters.Length == 0)
                throw new ArgumentException("There was no millisecond specified");

            DateTime valueOf = CreateDateTime(target.ToNumber());
            valueOf = valueOf.AddMilliseconds(-valueOf.Millisecond);
            valueOf = valueOf.AddMilliseconds(parameters[0].ToNumber());
            target.Value = New(valueOf).ToNumber();
            return target;
        }

        /// <summary>
        /// 15.9.5.30
        /// </summary>
        /// <param name="target"></param>
        /// <param name="parameters"></param>
        /// <returns></returns>
        public JsInstance SetSecondsImpl(JsDictionaryObject target, JsInstance[] parameters) {
            if (parameters.Length == 0)
                throw new ArgumentException("There was no second specified");
            DateTime valueOf = CreateDateTime(target.ToNumber()).ToLocalTime();
            valueOf = valueOf.AddSeconds(-valueOf.Second);
            valueOf = valueOf.AddSeconds(parameters[0].ToNumber());
            target.Value = valueOf;
            if (parameters.Length > 1) {
                JsInstance[] innerParams = new JsInstance[parameters.Length - 1];
                Array.Copy(parameters,1, innerParams, 0, innerParams.Length);
                target = (JsDate)SetMillisecondsImpl(target, innerParams);
            }
            return target;
        }

        /// <summary>
        /// 15.9.5.31
        /// </summary>
        /// <param name="target"></param>
        /// <param name="parameters"></param>
        /// <returns></returns>
        public JsInstance SetUTCSecondsImpl(JsDictionaryObject target, JsInstance[] parameters) {
            if (parameters.Length == 0)
                throw new ArgumentException("There was no second specified");
            DateTime valueOf = CreateDateTime(target.ToNumber());
            valueOf = valueOf.AddSeconds(-valueOf.Second);
            valueOf = valueOf.AddSeconds(parameters[0].ToNumber());
            target.Value = valueOf;
            if (parameters.Length > 1) {
                JsInstance[] innerParams = new JsInstance[parameters.Length - 1];
                Array.Copy(parameters, 1, innerParams, 0, innerParams.Length);
                target = (JsDate)SetMillisecondsImpl(target, innerParams);
            }
            return target;
        }

        /// <summary>
        /// 15.9.5.33
        /// </summary>
        /// <param name="target"></param>
        /// <param name="parameters"></param>
        /// <returns></returns>
        public JsInstance SetMinutesImpl(JsDictionaryObject target, JsInstance[] parameters) {
            if (parameters.Length == 0)
                throw new ArgumentException("There was no minute specified");
            DateTime valueOf = CreateDateTime(target.ToNumber()).ToLocalTime();
            valueOf = valueOf.AddMinutes(-valueOf.Minute);
            valueOf = valueOf.AddMinutes(parameters[0].ToNumber());
            target.Value = valueOf;
            if (parameters.Length > 1) {
                JsInstance[] innerParams = new JsInstance[parameters.Length - 1];
                Array.Copy(parameters, 1, innerParams, 0, innerParams.Length);
                target = (JsDate)SetSecondsImpl(target, innerParams);
            }
            return target;
        }

        /// <summary>
        /// 15.9.5.34
        /// </summary>
        /// <param name="target"></param>
        /// <param name="parameters"></param>
        /// <returns></returns>
        public JsInstance SetUTCMinutesImpl(JsDictionaryObject target, JsInstance[] parameters) {
            if (parameters.Length == 0)
                throw new ArgumentException("There was no minute specified");
            DateTime valueOf = CreateDateTime(target.ToNumber());
            valueOf = valueOf.AddMinutes(-valueOf.Minute);
            valueOf = valueOf.AddMinutes(parameters[0].ToNumber());
            target.Value = valueOf;
            if (parameters.Length > 1) {
                JsInstance[] innerParams = new JsInstance[parameters.Length - 1];
                Array.Copy(parameters, 1, innerParams, 0, innerParams.Length);
                target = (JsDate)SetSecondsImpl(target, innerParams);
            }
            return target;
        }

        /// <summary>
        /// 15.9.5.35
        /// </summary>
        /// <param name="target"></param>
        /// <param name="parameters"></param>
        /// <returns></returns>
        public JsInstance SetHoursImpl(JsDictionaryObject target, JsInstance[] parameters) {
            if (parameters.Length == 0)
                throw new ArgumentException("There was no hour specified");
            DateTime valueOf = CreateDateTime(target.ToNumber()).ToLocalTime();
            valueOf = valueOf.AddHours(-valueOf.Hour);
            valueOf = valueOf.AddHours(parameters[0].ToNumber());
            target.Value = valueOf;
            if (parameters.Length > 1) {
                JsInstance[] innerParams = new JsInstance[parameters.Length - 1];
                Array.Copy(parameters, 1, innerParams, 0, innerParams.Length);
                target = (JsDate)SetMinutesImpl(target, innerParams);
            }
            return target;
        }

        /// <summary>
        /// 15.9.5.36
        /// </summary>
        /// <param name="target"></param>
        /// <param name="parameters"></param>
        /// <returns></returns>
        public JsInstance SetUTCHoursImpl(JsDictionaryObject target, JsInstance[] parameters) {
            if (parameters.Length == 0)
                throw new ArgumentException("There was no hour specified");
            DateTime valueOf = CreateDateTime(target.ToNumber());
            valueOf = valueOf.AddHours(-valueOf.Hour);
            valueOf = valueOf.AddHours(parameters[0].ToNumber());
            target.Value = valueOf;
            if (parameters.Length > 1) {
                JsInstance[] innerParams = new JsInstance[parameters.Length - 1];
                Array.Copy(parameters, 1, innerParams, 0, innerParams.Length);
                target = (JsDate)SetMinutesImpl(target, innerParams);
            }
            return target;
        }

        /// <summary>
        /// 15.9.5.36
        /// </summary>
        /// <param name="target"></param>
        /// <param name="parameters"></param>
        /// <returns></returns>
        public JsInstance SetDateImpl(JsDictionaryObject target, JsInstance[] parameters) {
            if (parameters.Length == 0)
                throw new ArgumentException("There was no date specified");
            DateTime valueOf = CreateDateTime(target.ToNumber()).ToLocalTime();
            valueOf = valueOf.AddDays(-valueOf.Day);
            valueOf = valueOf.AddDays(parameters[0].ToNumber());
            target.Value = valueOf;
            return target;

        }

        /// <summary>
        /// 15.9.5.37
        /// </summary>
        /// <param name="target"></param>
        /// <param name="parameters"></param>
        /// <returns></returns>
        public JsInstance SetUTCDateImpl(JsDictionaryObject target, JsInstance[] parameters) {
            if (parameters.Length == 0)
                throw new ArgumentException("There was no date specified");
            DateTime valueOf = CreateDateTime(target.ToNumber());
            valueOf = valueOf.AddDays(-valueOf.Day);
            valueOf = valueOf.AddDays(parameters[0].ToNumber());
            target.Value = valueOf;
            return target;
        }

        /// <summary>
        /// 15.9.5.38
        /// </summary>
        /// <param name="target"></param>
        /// <param name="parameters"></param>
        /// <returns></returns>
        public JsInstance SetMonthImpl(JsDictionaryObject target, JsInstance[] parameters) {
            if (parameters.Length == 0)
                throw new ArgumentException("There was no month specified");
            DateTime valueOf = CreateDateTime(target.ToNumber()).ToLocalTime();
            valueOf = valueOf.AddMonths(-valueOf.Month);
            valueOf = valueOf.AddMonths((int)parameters[0].ToNumber() + 1);
            target.Value = valueOf;
            if (parameters.Length > 1) {
                JsInstance[] innerParams = new JsInstance[parameters.Length - 1];
                Array.Copy(parameters, 1, innerParams, 0, innerParams.Length);
                target = (JsDate)SetDateImpl(target, innerParams);
            }
            return target;
        }

        /// <summary>
        /// 15.9.5.39
        /// </summary>
        /// <param name="target"></param>
        /// <param name="parameters"></param>
        /// <returns></returns>
        public JsInstance SetUTCMonthImpl(JsDictionaryObject target, JsInstance[] parameters) {
            if (parameters.Length == 0)
                throw new ArgumentException("There was no month specified");
            DateTime valueOf = CreateDateTime(target.ToNumber());
            valueOf = valueOf.AddMonths(-valueOf.Month);
            valueOf = valueOf.AddMonths((int)parameters[0].ToNumber() + 1);
            target.Value = valueOf;
            if (parameters.Length > 1) {
                JsInstance[] innerParams = new JsInstance[parameters.Length - 1];
                Array.Copy(parameters, 1, innerParams, 0, innerParams.Length);
                target = (JsDate)SetDateImpl(target, innerParams);
            }
            return target;
        }

        /// <summary>
        /// 15.9.5.40
        /// </summary>
        /// <param name="target"></param>
        /// <param name="parameters"></param>
        /// <returns></returns>
        public JsInstance SetFullYearImpl(JsDictionaryObject target, JsInstance[] parameters) {
            if (parameters.Length == 0)
                throw new ArgumentException("There was no year specified");
            DateTime valueOf = CreateDateTime(target.ToNumber()).ToLocalTime();
            int diff = valueOf.Year - (int)parameters[0].ToNumber();
            valueOf = valueOf.AddYears(-diff);
            target.Value = valueOf;
            if (parameters.Length > 1) {
                JsInstance[] innerParams = new JsInstance[parameters.Length - 1];
                Array.Copy(parameters, 1, innerParams, 0, innerParams.Length);
                target = (JsDate)SetMonthImpl(target, innerParams);
            }
            return target;

        }

        /// <summary>
        /// 15.9.5.41
        /// </summary>
        /// <param name="target"></param>
        /// <param name="parameters"></param>
        /// <returns></returns>
        public JsInstance SetUTCFullYearImpl(JsDictionaryObject target, JsInstance[] parameters) {
            if (parameters.Length == 0)
                throw new ArgumentException("There was no year specified");
            DateTime valueOf = CreateDateTime(target.ToNumber());
            valueOf = valueOf.AddYears(-valueOf.Year);
            valueOf = valueOf.AddYears((int)parameters[0].ToNumber());
            target.Value = valueOf;
            if (parameters.Length > 1) {
                JsInstance[] innerParams = new JsInstance[parameters.Length - 1];
                Array.Copy(parameters, 1, innerParams, 0, innerParams.Length);
                target = (JsDate)SetMonthImpl(target, innerParams);
            }
            return target;
        }

        public JsInstance UTCImpl(JsInstance[] parameters) {
            for (int i = 0; i < parameters.Length; i++) {
                if (parameters[i] == JsUndefined.Instance  // undefined
                    || (parameters[i].Class == JsInstance.CLASS_NUMBER && double.IsNaN(parameters[i].ToNumber())) // NaN
                    || (parameters[i].Class == JsInstance.CLASS_NUMBER && double.IsInfinity(parameters[i].ToNumber())) // Infinity
                    //|| parameters[i].Class == JsInstance.CLASS_OBJECT // don't accept objects ???!
                    ) {
                    return Global.NaN;
                }
            }

            JsDate result = Construct(parameters);
            double offset = result.ToNumber() + TimeZone.CurrentTimeZone.GetUtcOffset(new DateTime()).TotalMilliseconds;
            return Global.NumberClass.New(offset);
        }

    }
}
