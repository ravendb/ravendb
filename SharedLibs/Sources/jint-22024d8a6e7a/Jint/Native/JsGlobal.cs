using System;
using Jint.Expressions;
using System.Globalization;
using System.Text.RegularExpressions;

namespace Jint.Native {
    [Serializable]
    public class JsGlobal : JsObject, IGlobal {
        /// <summary>
        /// Useful for eval()
        /// </summary>
        public IJintVisitor Visitor { get; set; }

        public Options Options { get; set; }

        public JsGlobal(ExecutionVisitor visitor, Options options)
            : base(JsNull.Instance) {
            this.Options = options;
            this.Visitor = visitor;

            this["null"] = JsNull.Instance;
            JsObject objectProrotype = new JsObject(JsNull.Instance);

            JsFunction functionPrototype = new JsFunctionWrapper(
                delegate(JsInstance[] arguments) {
                    return JsUndefined.Instance;
                },
                objectProrotype
            );

            Marshaller = new Marshaller(this);

            #region Global Classes
            this["Function"] = FunctionClass = new JsFunctionConstructor(this, functionPrototype);
            this["Object"] = ObjectClass = new JsObjectConstructor(this, functionPrototype, objectProrotype);
            ObjectClass.InitPrototype(this);


            this["Array"] = ArrayClass = new JsArrayConstructor(this);
            this["Boolean"] = BooleanClass = new JsBooleanConstructor(this);
            this["Date"] = DateClass = new JsDateConstructor(this);

            this["Error"] = ErrorClass = new JsErrorConstructor(this, "Error");
            this["EvalError"] = EvalErrorClass = new JsErrorConstructor(this, "EvalError");
            this["RangeError"] = RangeErrorClass = new JsErrorConstructor(this, "RangeError");
            this["ReferenceError"] = ReferenceErrorClass = new JsErrorConstructor(this, "ReferenceError");
            this["SyntaxError"] = SyntaxErrorClass = new JsErrorConstructor(this, "SyntaxError");
            this["TypeError"] = TypeErrorClass = new JsErrorConstructor(this, "TypeError");
            this["URIError"] = URIErrorClass = new JsErrorConstructor(this, "URIError");

            this["Number"] = NumberClass = new JsNumberConstructor(this);
            this["RegExp"] = RegExpClass = new JsRegExpConstructor(this);
            this["String"] = StringClass = new JsStringConstructor(this);
            this["Math"] = MathClass = new JsMathConstructor(this);

            // 15.1 prototype of the global object varies on the implementation
            //this.Prototype = ObjectClass.PrototypeProperty;
            #endregion


            foreach (JsInstance c in this.GetValues()) {
                if (c is JsConstructor) {
                    ((JsConstructor)c).InitPrototype(this);
                }
            }

            #region Global Properties
            this["NaN"] = NumberClass["NaN"];  // 15.1.1.1
            this["Infinity"] = NumberClass["POSITIVE_INFINITY"]; // // 15.1.1.2
            this["undefined"] = JsUndefined.Instance; // 15.1.1.3
            this[JsScope.THIS] = this;
            #endregion

            #region Global Functions
            // every embed function should have a prototype FunctionClass.PrototypeProperty - 15.
            this["eval"] = new JsFunctionWrapper(Eval, FunctionClass.PrototypeProperty); // 15.1.2.1
            this["parseInt"] = new JsFunctionWrapper(ParseInt, FunctionClass.PrototypeProperty); // 15.1.2.2
            this["parseFloat"] = new JsFunctionWrapper(ParseFloat, FunctionClass.PrototypeProperty); // 15.1.2.3
            this["isNaN"] = new JsFunctionWrapper(IsNaN, FunctionClass.PrototypeProperty);
            this["isFinite"] = new JsFunctionWrapper(isFinite, FunctionClass.PrototypeProperty);
            this["decodeURI"] = new JsFunctionWrapper(DecodeURI, FunctionClass.PrototypeProperty);
            this["encodeURI"] = new JsFunctionWrapper(EncodeURI, FunctionClass.PrototypeProperty);
            this["decodeURIComponent"] = new JsFunctionWrapper(DecodeURIComponent, FunctionClass.PrototypeProperty);
            this["encodeURIComponent"] = new JsFunctionWrapper(EncodeURIComponent, FunctionClass.PrototypeProperty);
            #endregion

            Marshaller.InitTypes();

        }

        public override string Class
        {
            get
            {
                return CLASS_GLOBAL;
            }
        }

        #region Global Functions

        public JsObjectConstructor ObjectClass { get; private set; }
        public JsFunctionConstructor FunctionClass { get; private set; }
        public JsArrayConstructor ArrayClass { get; private set; }
        public JsBooleanConstructor BooleanClass { get; private set; }
        public JsDateConstructor DateClass { get; private set; }
        public JsErrorConstructor ErrorClass { get; private set; }
        public JsErrorConstructor EvalErrorClass { get; private set; }
        public JsErrorConstructor RangeErrorClass { get; private set; }
        public JsErrorConstructor ReferenceErrorClass { get; private set; }
        public JsErrorConstructor SyntaxErrorClass { get; private set; }
        public JsErrorConstructor TypeErrorClass { get; private set; }
        public JsErrorConstructor URIErrorClass { get; private set; }

        public JsMathConstructor MathClass { get; private set; }
        public JsNumberConstructor NumberClass { get; private set; }
        public JsRegExpConstructor RegExpClass { get; private set; }
        public JsStringConstructor StringClass { get; private set; }
        public Marshaller Marshaller { get; private set; }

        /// <summary>
        /// 15.1.2.1
        /// </summary>
        public JsInstance Eval(JsInstance[] arguments) {
            if (JsInstance.CLASS_STRING != arguments[0].Class) {
                return arguments[0];
            }

            Program p;

            try {
                p = JintEngine.Compile(arguments[0].ToString(), Visitor.DebugMode);
            }
            catch (Exception e) {
                throw new JsException(this.SyntaxErrorClass.New(e.Message));
            }

            try {
                p.Accept((IStatementVisitor)Visitor);
            }
            catch (Exception e) {
                throw new JsException(this.EvalErrorClass.New(e.Message));
            }

            return Visitor.Result;
        }

        /// <summary>
        /// 15.1.2.2
        /// </summary>
        public JsInstance ParseInt(JsInstance[] arguments) {
            if (arguments.Length < 1 || arguments[0] == JsUndefined.Instance) {
                return JsUndefined.Instance;
            }

            //in case of an enum, just cast it to an integer
            if (arguments[0].IsClr && arguments[0].Value.GetType().IsEnum)
                return NumberClass.New((int)arguments[0].Value);

            string number = arguments[0].ToString().Trim();
            int sign = 1;
            int radix = 10;

            if (number == String.Empty) {
                return this["NaN"];
            }

            if (number.StartsWith("-")) {
                number = number.Substring(1);
                sign = -1;
            }
            else if (number.StartsWith("+")) {
                number = number.Substring(1);
            }

            if (arguments.Length >= 2) {
                if (arguments[1] != JsUndefined.Instance && !0.Equals(arguments[1])) {
                    radix = Convert.ToInt32(arguments[1].Value);
                }
            }

            if (radix == 0) {
                radix = 10;
            }
            else if (radix < 2 || radix > 36) {
                return this["NaN"];
            }

            if (number.ToLower().StartsWith("0x")) {
                radix = 16;
            }

            try {
                if (radix == 10) {
                    // most common case
                    double result;
                    if(double.TryParse(number,NumberStyles.Any, CultureInfo.InvariantCulture, out result)) {
                        // parseInt(12.42) == 42
                        return NumberClass.New(sign * Math.Floor(result));
                    }
                    else {
                        return this["NaN"];
                    }
                }
                else {
                    return NumberClass.New(sign * Convert.ToInt32(number, radix));
                }
            }
            catch {
                return this["NaN"];
            }
        }

        /// <summary>
        /// 15.1.2.3
        /// </summary>
        public JsInstance ParseFloat(JsInstance[] arguments) {
            if (arguments.Length < 1 || arguments[0] == JsUndefined.Instance) {
                return JsUndefined.Instance;
            }

            string number = arguments[0].ToString().Trim();
            // the parseFloat function should stop parsing when it encounters an unalowed char
            Regex regexp = new Regex(@"^[\+\-\d\.e]*", RegexOptions.IgnoreCase);

            Match match = regexp.Match(number);

            double result;
            if (match.Success && double.TryParse(match.Value, NumberStyles.Float, new CultureInfo("en-US"), out result)) {
                return NumberClass.New(result);
            }
            else {
                return this["NaN"];
            }
        }

        /// <summary>
        /// 15.1.2.4
        /// </summary>
        public JsInstance IsNaN(JsInstance[] arguments) {
            if (arguments.Length < 1) {
                return BooleanClass.New(false);
            }

            return BooleanClass.New(double.NaN.Equals(arguments[0].ToNumber()));
        }

        /// <summary>
        /// 15.1.2.5
        /// </summary>
        protected JsInstance isFinite(JsInstance[] arguments) {
            if (arguments.Length < 1 || arguments[0] == JsUndefined.Instance) {
                return BooleanClass.False;
            }

            var value = arguments[0];
            return BooleanClass.New((value != NumberClass["NaN"]
                && value != NumberClass["POSITIVE_INFINITY"]
                && value != NumberClass["NEGATIVE_INFINITY"])
            );
        }

        protected JsInstance DecodeURI(JsInstance[] arguments) {
            if (arguments.Length < 1 || arguments[0] == JsUndefined.Instance) {
                return StringClass.New();
            }

            return this.StringClass.New(Uri.UnescapeDataString(arguments[0].ToString().Replace("+", " ")));
        }

        private static char[] reservedEncoded = new char[] { ';', ',', '/', '?', ':', '@', '&', '=', '+', '$', '#' };
        private static char[] reservedEncodedComponent = new char[] { '-', '_', '.', '!', '~', '*', '\'', '(', ')', '[', ']' };

        protected JsInstance EncodeURI(JsInstance[] arguments) {
            if (arguments.Length < 1 || arguments[0] == JsUndefined.Instance) {
                return this.StringClass.New();
            }

            string encoded = Uri.EscapeDataString(arguments[0].ToString());

            foreach (char c in reservedEncoded) {
                encoded = encoded.Replace(Uri.EscapeDataString(c.ToString()), c.ToString());
            }

            foreach (char c in reservedEncodedComponent) {
                encoded = encoded.Replace(Uri.EscapeDataString(c.ToString()), c.ToString());
            }

            return this.StringClass.New(encoded.ToUpper());
        }

        protected JsInstance DecodeURIComponent(JsInstance[] arguments) {
            if (arguments.Length < 1 || arguments[0] == JsUndefined.Instance) {
                return this.StringClass.New();
            }

            return this.StringClass.New(Uri.UnescapeDataString(arguments[0].ToString().Replace("+", " ")));
        }

        protected JsInstance EncodeURIComponent(JsInstance[] arguments) {
            if (arguments.Length < 1 || arguments[0] == JsUndefined.Instance) {
                return this.StringClass.New();
            }

            string encoded = Uri.EscapeDataString(arguments[0].ToString());

            foreach (char c in reservedEncodedComponent) {
                encoded = encoded.Replace(Uri.EscapeDataString(c.ToString()), c.ToString().ToUpper());
            }

            return this.StringClass.New(encoded);
        }

        #endregion
        [Obsolete]
        public JsObject Wrap(object value) {
            switch (Convert.GetTypeCode(value)) {
                case TypeCode.Boolean:
                    return BooleanClass.New((bool)value);
                case TypeCode.Char:
                case TypeCode.String:
                    return StringClass.New(Convert.ToString(value));
                case TypeCode.DateTime:
                    return DateClass.New((DateTime)value);
                case TypeCode.Byte:
                case TypeCode.Int16:
                case TypeCode.Int32:
                case TypeCode.Int64:
                case TypeCode.SByte:
                case TypeCode.UInt16:
                case TypeCode.UInt32:
                case TypeCode.UInt64:
                case TypeCode.Decimal:
                case TypeCode.Double:
                case TypeCode.Single:
                    return NumberClass.New(Convert.ToDouble(value));
                case TypeCode.Object:
                    return ObjectClass.New(value);
                case TypeCode.DBNull:
                case TypeCode.Empty:
                default:
                    throw new ArgumentNullException("value");
            }
        }

        public JsObject WrapClr(object value) {
            return (JsObject)Marshaller.MarshalClrValue<object>(value);
        }

        public bool HasOption(Options options) {
            return (Options & options) == options;
        }

        #region IGlobal Members


        public JsInstance NaN {
            get { return this["NaN"]; }
        }

        #endregion
    }
}
