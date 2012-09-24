using System;

namespace Jint.Native {
  
    [Serializable]
    public class JsMathConstructor : JsObject {
        public IGlobal Global { get; set; }

        public JsMathConstructor(IGlobal global)
            : base(global.ObjectClass.PrototypeProperty) {
            Global = global;
            var random = new Random();

            #region Functions
            this["abs"] = global.FunctionClass.New(new Func<double, JsNumber>(d => Global.NumberClass.New(Math.Abs(d))));
            this["acos"] = global.FunctionClass.New(new Func<double, JsNumber>(d => Global.NumberClass.New(Math.Acos(d))));
            this["asin"] = global.FunctionClass.New(new Func<double, JsNumber>(d => Global.NumberClass.New(Math.Asin(d))));
            this["atan"] = global.FunctionClass.New(new Func<double, JsNumber>(d => Global.NumberClass.New(Math.Atan(d))));
            this["atan2"] = global.FunctionClass.New(new Func<double, double, JsNumber>((y, x) => Global.NumberClass.New(Math.Atan2(y, x))));
            this["ceil"] = global.FunctionClass.New(new Func<double, JsNumber>(d => Global.NumberClass.New(Math.Ceiling(d))));
            this["cos"] = global.FunctionClass.New(new Func<double, JsNumber>(d => Global.NumberClass.New(Math.Cos(d))));
            this["exp"] = global.FunctionClass.New(new Func<double, JsNumber>(d => Global.NumberClass.New(Math.Exp(d))));
            this["floor"] = global.FunctionClass.New(new Func<double, JsNumber>(d => Global.NumberClass.New(Math.Floor(d))));
            this["log"] = global.FunctionClass.New(new Func<double, JsNumber>(d => Global.NumberClass.New(Math.Log(d))));
            this["max"] = global.FunctionClass.New<JsObject>(MaxImpl);
            this["min"] = global.FunctionClass.New<JsObject>(MinImpl);
            this["pow"] = global.FunctionClass.New(new Func<double, double, JsNumber>((a, b) => Global.NumberClass.New(Math.Pow(a, b))));
            this["random"] = global.FunctionClass.New(new Func<double>(random.NextDouble));
            this["round"] = global.FunctionClass.New(new Func<double, JsNumber>(d => Global.NumberClass.New(Math.Round(d))));
            this["sin"] = global.FunctionClass.New(new Func<double, JsNumber>(d => Global.NumberClass.New(Math.Sin(d))));
            this["sqrt"] = global.FunctionClass.New(new Func<double, JsNumber>(d => Global.NumberClass.New(Math.Sqrt(d))));
            this["tan"] = global.FunctionClass.New(new Func<double, JsNumber>(d => Global.NumberClass.New(Math.Tan(d))));
            #endregion

            this["E"] = global.NumberClass.New(Math.E);
            this["LN2"] = global.NumberClass.New(Math.Log(2));
            this["LN10"] = global.NumberClass.New(Math.Log(10));
            this["LOG2E"] = global.NumberClass.New(Math.Log(Math.E, 2));
            this["PI"] = global.NumberClass.New(Math.PI);
            this["SQRT1_2"] = global.NumberClass.New(Math.Sqrt(0.5));
            this["SQRT2"] = global.NumberClass.New(Math.Sqrt(2));
        }

        public const string MathType = "object";

        public override string Class {
            get { return MathType; }
        }

        public JsInstance MaxImpl(JsObject target, JsInstance[] parameters)
        {
            if (parameters.Length == 0) {
                return Global.NumberClass["NEGATIVE_INFINITY"];
            }

            var result = parameters[0].ToNumber();

            foreach (var p in parameters)
            {
                result = Math.Max(p.ToNumber(), result);
            }

            return Global.NumberClass.New(result);
        }


        public JsInstance MinImpl(JsObject target, JsInstance[] parameters)
        {
            if (parameters.Length == 0)
            {
                return Global.NumberClass["POSITIVE_INFINITY"];
            }

            var result = parameters[0].ToNumber();

            foreach (var p in parameters)
            {
                result = Math.Min(p.ToNumber(), result);
            }

            return Global.NumberClass.New(result);
        }
    }
}
