using System;
using System.Collections.Generic;
using System.Text;
using System.Globalization;
using Jint.Expressions;

namespace Jint.Native {
    [Serializable]
    public class JsNumberConstructor : JsConstructor {
        public JsNumberConstructor(IGlobal global)
            : base(global) {
            Name = "Number";

            DefineOwnProperty(PROTOTYPE, global.ObjectClass.New(this), PropertyAttributes.ReadOnly | PropertyAttributes.DontEnum | PropertyAttributes.DontDelete);

            this.DefineOwnProperty("MAX_VALUE", New(Double.MaxValue));
            this.DefineOwnProperty("MIN_VALUE", New(Double.MinValue));
            this.DefineOwnProperty("NaN", New(Double.NaN));
            this.DefineOwnProperty("NEGATIVE_INFINITY", New(Double.PositiveInfinity));
            this.DefineOwnProperty("POSITIVE_INFINITY", New(Double.NegativeInfinity));
        }

        public override void InitPrototype(IGlobal global) {
            var Prototype = PrototypeProperty;

            Prototype.DefineOwnProperty("toString", global.FunctionClass.New<JsInstance>(ToStringImpl,1), PropertyAttributes.DontEnum);
            Prototype.DefineOwnProperty("toLocaleString", global.FunctionClass.New<JsInstance>(ToLocaleStringImpl), PropertyAttributes.DontEnum);
            Prototype.DefineOwnProperty("toFixed", global.FunctionClass.New<JsInstance>(ToFixedImpl), PropertyAttributes.DontEnum);
            Prototype.DefineOwnProperty("toExponential", global.FunctionClass.New<JsInstance>(ToExponentialImpl), PropertyAttributes.DontEnum);
            Prototype.DefineOwnProperty("toPrecision", global.FunctionClass.New<JsInstance>(ToPrecisionImpl), PropertyAttributes.DontEnum);
        }

        public JsNumber New(double value) {
            return new JsNumber(value, PrototypeProperty);
        }

        public JsNumber New() {
            return New(0d);
        }

        public override JsInstance Execute(IJintVisitor visitor, JsDictionaryObject that, JsInstance[] parameters) {
            if (that == null || (that as IGlobal) == visitor.Global)
            {
                // 15.7.1 - When Number is called as a function rather than as a constructor, it performs a type conversion.
                if (parameters.Length > 0) {
                    return visitor.Return(New(parameters[0].ToNumber()));
                }
                else {
                    return visitor.Return(New(0));
                }
            }
            else {
                // 15.7.2 - When Number is called as part of a new expression, it is a constructor: it initialises the newly created object.
                if (parameters.Length > 0) {
                    that.Value = parameters[0].ToNumber();
                }
                else {
                    that.Value = 0;
                }

                return visitor.Return(that);
            }
        }

        public JsInstance ToLocaleStringImpl(JsInstance target, JsInstance[] parameters) {
            // Remove parameters
            return ToStringImpl(target, new JsInstance[0]);
        }

        private static char[] rDigits = {
        '0', '1', '2', '3', '4', '5', '6', '7', '8', '9', 
        'A', 'B', 'C', 'D', 'E', 'F', 'G', 'H', 'I', 'J', 
        'K', 'L', 'M', 'N', 'O', 'P', 'Q', 'R', 'S', 'T', 
        'U', 'V', 'W', 'X', 'Y', 'Z' };

        public JsInstance ToStringImpl(JsInstance target, JsInstance[] parameters) {
            if (target == this["NaN"]) {
                return Global.StringClass.New("NaN");
            }

            if (target == this["NEGATIVE_INFINITY"]) {
                return Global.StringClass.New("-Infinity");
            }

            if (target == this["POSITIVE_INFINITY"]) {
                return Global.StringClass.New("Infinity");
            }

            int radix = 10;

            // is radix defined ?
            if (parameters.Length > 0) {
                if (parameters[0] != JsUndefined.Instance) {
                    radix = (int)parameters[0].ToNumber();
                }
            }

            var longToBeFormatted = (long)target.ToNumber();

            if (radix == 10) {
                return Global.StringClass.New(target.ToNumber().ToString(CultureInfo.InvariantCulture).ToLower());
            }
            else {
                // Extract the magnitude for conversion.
                long longPositive = Math.Abs(longToBeFormatted);
                int digitIndex = 0;

                char[] outDigits = new char[63];
                // Convert the magnitude to a digit string.
                for (digitIndex = 0; digitIndex <= 64; digitIndex++)
                {
                    if (longPositive == 0) break;

                    outDigits[outDigits.Length - digitIndex - 1] =
                        rDigits[longPositive % radix];
                    longPositive /= radix;
                }

                // Add a minus sign if the argument is negative.
                if (longToBeFormatted < 0)
                    outDigits[outDigits.Length - digitIndex++ - 1] = '-';

                return Global.StringClass.New(new string(outDigits,
                    outDigits.Length - digitIndex, digitIndex).ToLower());
            }
        }

        /// <summary>
        /// 15.7.4.5
        /// </summary>
        /// <param name="target"></param>
        /// <param name="parameters"></param>
        /// <returns></returns>
        public JsInstance ToFixedImpl(JsInstance target, JsInstance[] parameters) {
            int fractions = 0;
            if (parameters.Length > 0) {
                fractions = Convert.ToInt32(parameters[0].ToNumber());
            }

            if (fractions > 20 || fractions < 0) {
                throw new JsException(Global.SyntaxErrorClass.New("Fraction Digits must be greater than 0 and lesser than 20"));
            }

            if (target == Global.NaN) {
                return Global.StringClass.New(target.ToString());
            }

            return Global.StringClass.New(target.ToNumber().ToString("f" + fractions, CultureInfo.InvariantCulture));
        }

        /// <summary>
        /// 15.7.4.6
        /// </summary>
        /// <param name="target"></param>
        /// <param name="parameters"></param>
        /// <returns></returns>
        public JsInstance ToExponentialImpl(JsInstance target, JsInstance[] parameters) {
            if (double.IsInfinity(target.ToNumber()) || double.IsNaN(target.ToNumber())) {
                return ToStringImpl(target, new JsInstance[0]);
            }

            int fractions = 16;
            if (parameters.Length > 0) {
                fractions = Convert.ToInt32(parameters[0].ToNumber());
            }

            if (fractions > 20 || fractions < 0) {
                throw new JsException(Global.SyntaxErrorClass.New("Fraction Digits must be greater than 0 and lesser than 20"));
            }

            string format = String.Concat("#.", new String('0', fractions), "e+0");
            return Global.StringClass.New(target.ToNumber().ToString(format, CultureInfo.InvariantCulture));
        }

        /// <summary>
        /// 15.7.4.7
        /// </summary>
        /// <param name="target"></param>
        /// <param name="parameters"></param>
        /// <returns></returns>
        public JsInstance ToPrecisionImpl(JsInstance target, JsInstance[] parameters) {
            if (double.IsInfinity(target.ToNumber()) || double.IsNaN(target.ToNumber())) {
                return ToStringImpl(target, new JsInstance[0]);
            }

            if (parameters.Length == 0) {
                throw new JsException(Global.SyntaxErrorClass.New("precision missing"));
            }

            if (parameters[0] == JsUndefined.Instance) {
                return ToStringImpl(target, new JsInstance[0]);
            }

            int precision = 0;
            if (parameters.Length > 0) {
                precision = Convert.ToInt32(parameters[0].ToNumber());
            }

            if (precision < 1 || precision > 21) {
                throw new JsException(Global.RangeErrorClass.New("precision must be between 1 and 21"));
            }

            // Get the number of decimals
            string str = target.ToNumber().ToString("e23", CultureInfo.InvariantCulture);
            int decimals = str.IndexOfAny(new char[] { '.', 'e' });
            decimals = decimals == -1 ? str.Length : decimals;

            precision -= decimals;
            precision = precision < 1 ? 1 : precision;

            return Global.StringClass.New(target.ToNumber().ToString("f" + precision, CultureInfo.InvariantCulture));
        }
    }
}
