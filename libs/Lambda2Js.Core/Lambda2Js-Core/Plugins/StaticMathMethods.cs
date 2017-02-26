using System;
using System.Collections.Generic;
using System.Linq.Expressions;

namespace Lambda2Js
{
    public class StaticMathMethods : JavascriptConversionExtension
    {
        struct NameLength
        {
            public string name;
            public byte length;
        }

        // Equivalence table between javascript and .Net
        //
        // .Net methods         JavaScript functions
        // -------------------  --------------------
        // Math.Abs             Math.abs
        // Math.Acos            Math.acos
        //                      Math.acosh
        // Math.Asin            Math.asin
        //                      Math.asinh
        // Math.Atan            Math.atan
        //                      Math.atanh
        // Math.Atan2           Math.atan2
        //                      Math.cbrt
        // Math.Ceiling         Math.ceil
        //                      Math.clz32
        // Math.Cos             Math.cos
        // Math.Cosh            Math.cosh
        // Math.Exp             Math.exp
        // Math.Floor           Math.floor
        //                      Math.expm1
        //                      Math.fround
        //                      Math.ipot
        //                      Math.imul
        // Math.IEEERemainder
        // Math.Log             Math.log
        // Math.Log10           Math.log10
        //                      Math.log1p
        //                      Math.log2
        // Math.Max             Math.max
        // Math.Min             Math.min
        // Math.Pow             Math.pow
        //                      Math.random
        // Math.Round           Math.round
        // Math.Sign            Math.sign
        // Math.Sin             Math.sin
        // Math.Sinh            Math.sinh
        // Math.Sqrt            Math.sqrt
        // Math.Tan             Math.tan
        // Math.Tanh            Math.tanh
        //                      Math.trunc
        //
        // .Net constants       JavaScript constants
        // -------------------  ---------------------
        // Math.E               Math.E
        //                      Math.LN10
        //                      Math.LN2
        //                      Math.LOG10E
        //                      Math.LOG2E
        // Math.PI              Math.PI
        //                      Math.SQRT1_2
        //                      Math.SQRT2
        private static readonly Dictionary<string, NameLength> membersMap = new Dictionary<string, NameLength>
            {
                { "Abs", new NameLength { name = "abs",     length = 1 } },
                { "Acos", new NameLength { name = "acos",   length = 1 } },
                { "Asin", new NameLength { name = "asin",   length = 1 } },
                { "Atan", new NameLength { name = "atan",   length = 1 } },
                { "Atan2", new NameLength { name = "atan2", length = 2 } },
                { "Ceiling", new NameLength { name = "ceil",length = 1 } },
                { "Cos", new NameLength { name = "cos",     length = 1 } },
                { "Cosh", new NameLength { name = "cosh",   length = 1 } },
                { "Exp", new NameLength { name = "exp",     length = 1 } },
                { "Floor", new NameLength { name = "floor", length = 1 } },
                { "Log", new NameLength { name = "log",     length = 1 } },
                { "Log10", new NameLength { name = "log10", length = 1 } },
                { "Max", new NameLength { name = "max",     length = 2 } },
                { "Min", new NameLength { name = "min",     length = 2 } },
                { "Pow", new NameLength { name = "pow",     length = 2 } },
                { "Round", new NameLength { name = "round", length = 1 } },
                { "Sign", new NameLength { name = "sign",   length = 1 } },
                { "Sin", new NameLength { name = "sin",     length = 1 } },
                { "Sinh", new NameLength { name = "sinh",   length = 1 } },
                { "Sqrt", new NameLength { name = "sqrt",   length = 1 } },
                { "Tan", new NameLength { name = "tan",     length = 1 } },
                { "Tanh", new NameLength { name = "tanh",   length = 1 } },
            };

        private bool round2;

        public StaticMathMethods(bool round2 = false, bool catchEandPI = false)
        {
            this.round2 = round2;
        }

        public override void ConvertToJavascript(JavascriptConversionContext context)
        {
            var methodCall = context.Node as MethodCallExpression;
            if (methodCall != null)
                if (methodCall.Method.DeclaringType == typeof(Math))
                {
                    NameLength jsInfo;
                    if (membersMap.TryGetValue(methodCall.Method.Name, out jsInfo))
                        if (methodCall.Arguments.Count == jsInfo.length)
                        {
                            context.PreventDefault();
                            using (context.Operation(JavascriptOperationTypes.Call))
                            {
                                using (context.Operation(JavascriptOperationTypes.IndexerProperty))
                                    context.Write("Math." + jsInfo.name);
                                context.WriteManyIsolated('(', ')', ',', methodCall.Arguments);
                            }

                            return;
                        }
                        else if (methodCall.Method.Name == "Log"
                            && methodCall.Arguments.Count == 2)
                        {
                            // JavaScript does not support `Math.log` with 2 parameters,
                            // But it is easy enough for us to give a little help!
                            context.PreventDefault();
                            using (context.Operation(JavascriptOperationTypes.MulDivMod))
                            {
                                using (context.Operation(JavascriptOperationTypes.Call))
                                {
                                    using (context.Operation(JavascriptOperationTypes.IndexerProperty))
                                        context.Write("Math.log");
                                    context.Write('(');
                                    using (context.Operation(0))
                                        context.Write(methodCall.Arguments[0]);
                                    context.Write(')');
                                }

                                context.Write('/');

                                using (context.Operation(JavascriptOperationTypes.Call))
                                {
                                    using (context.Operation(JavascriptOperationTypes.IndexerProperty))
                                        context.Write("Math.log");
                                    context.Write('(');
                                    using (context.Operation(0))
                                        context.Write(methodCall.Arguments[1]);
                                    context.Write(')');
                                }
                            }

                            return;
                        }
                        else if (methodCall.Method.Name == "Round"
                            && methodCall.Arguments.Count == 2
                            && TypeHelpers.IsNumericType(methodCall.Arguments[1].Type))
                        {
                            // We won't support `Math.Round` with two parameters by default.
                            // To do it, we'd have to repeat an input value in the expression (unacceptable):
                            //      Math.Round(A, B) => Math.round(A * Math.pow(10, B)) / Math.pow(10, B)
                            // Or start helping with hacky things (acceptable, but not by default):
                            //      Math.Round(A, B) => (function(a, b) { return Math.round(a * b) / b; })(A, Math.pow(10, B));
                            if (this.round2)
                            {
                                context.PreventDefault();
                                using (context.Operation(JavascriptOperationTypes.Call))
                                {
                                    context.WriteLambda<Func<double, double, double>>((a, b) => Math.Round(a * b) / b);
                                    context.Write('(');
                                    using (context.Operation(0))
                                        context.Write(methodCall.Arguments[0]);
                                    context.Write(',');
                                    using (context.Operation(0))
                                    using (context.Operation(JavascriptOperationTypes.Call))
                                    {
                                        using (context.Operation(JavascriptOperationTypes.IndexerProperty))
                                            context.Write("Math.pow");
                                        context.Write('(');
                                        context.Write("10");
                                        context.Write(',');
                                        using (context.Operation(0))
                                            context.Write(methodCall.Arguments[1]);
                                        context.Write(')');
                                    }

                                    context.Write(')');

                                    return;
                                }
                            }
                        }
                }

            // E and PI are constant values, they will never result in
            // a member access expression. We will have to catch the
            // exact numbers, and convert them instead.
            var constVal = context.Node as ConstantExpression;
            if (constVal != null)
                if (constVal.Value.Equals(Math.E))
                {
                    context.PreventDefault();
                    using (context.Operation(JavascriptOperationTypes.IndexerProperty))
                        context.Write("Math.E");
                }
                else if (constVal.Value.Equals(Math.PI))
                {
                    context.PreventDefault();
                    using (context.Operation(JavascriptOperationTypes.IndexerProperty))
                        context.Write("Math.PI");
                }
        }
    }
}
