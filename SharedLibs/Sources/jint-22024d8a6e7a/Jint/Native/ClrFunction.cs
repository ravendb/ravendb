using System;
using System.Collections.Generic;
using System.Text;
using Jint.Expressions;
using System.Reflection;
using System.Reflection.Emit;

namespace Jint.Native {
    // TODO: remove, since this is no longer used
    /// <summary>
    /// Wraps native function, i.e. 'this' parameter from the js calling context will be ommited
    /// </summary>
    [Serializable]
    public class ClrFunction : JsFunction {
        public Delegate Delegate { get; set; }
        public ParameterInfo[] Parameters { get; set; }

        public ClrFunction(Delegate d, JsObject prototype)
            : base(prototype) {
            Delegate = d;
            Parameters = d.Method.GetParameters();
        }

        public override JsInstance Execute(IJintVisitor visitor, JsDictionaryObject that, JsInstance[] parameters) {
            int clrParameterCount = Delegate.Method.GetParameters().Length;
            object[] clrParameters = new object[clrParameterCount];

            for (int i = 0; i < parameters.Length; i++) {
                // First see if either the JsInstance or it's value can be directly accepted without converstion
                if (typeof(JsInstance).IsAssignableFrom(Parameters[i].ParameterType) && Parameters[i].ParameterType.IsInstanceOfType(parameters[i])) {
                    clrParameters[i] = parameters[i];
                } else if (Parameters[i].ParameterType.IsInstanceOfType(parameters[i].Value)) {
                    clrParameters[i] = parameters[i].Value;
                } else {
                    clrParameters[i] = visitor.Global.Marshaller.MarshalJsValue<object>(parameters[i]);
                }
            }

            object result;

            try {
                result = Delegate.DynamicInvoke(clrParameters);
            }
            catch (TargetInvocationException e) {
                throw e.InnerException;
            }
            catch (Exception e) {
                if (e.InnerException is JsException) {
                    throw e.InnerException;
                }

                throw;
            }

            if (result != null) {
                // Don't wrap if the result should be a JsInstance
                if (typeof(JsInstance).IsInstanceOfType(result)) {
                    visitor.Return((JsInstance)result);
                }
                else {
                    visitor.Return(visitor.Global.WrapClr(result));
                }
            }
            else {
                visitor.Return(JsUndefined.Instance);
            }

            return null;
        }

        public override string ToString() {
            return String.Format("function {0}() { [native code] }", Delegate.Method.Name);
        }
    }
}
