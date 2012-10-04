using System;
using System.Collections.Generic;
using System.Text;
using Jint.Expressions;

namespace Jint.Native {
    /// <summary>
    /// This class is used to model the function.call behaviour, which takes two arguments: this, and the parameters
    /// It is defined in Function.prototype so that every function can use it by default
    /// </summary>
    [Serializable]
    public class JsApplyFunction : JsFunction {
        public JsApplyFunction(JsFunctionConstructor constructor)
            : base(constructor.PrototypeProperty) {
            DefineOwnProperty("length", constructor.Global.NumberClass.New(2), PropertyAttributes.ReadOnly);
        }

        public override JsInstance Execute(IJintVisitor visitor, JsDictionaryObject that, JsInstance[] parameters) {
            JsFunction function = that as JsFunction;

            if (function == null) {
                throw new ArgumentException("the target of call() must be a function");
            }
            JsDictionaryObject _this;
            JsInstance[] _parameters;
            if (parameters.Length >= 1)
                _this = parameters[0] as JsDictionaryObject;
            else
                _this = visitor.Global as JsDictionaryObject;

            if (parameters.Length >= 2 && parameters[1] != JsNull.Instance) {
                JsObject arguments = parameters[1] as JsObject;
                if (arguments == null)
                    throw new JsException(visitor.Global.TypeErrorClass.New("second argument must be an array"));
                _parameters = new JsInstance[arguments.Length];
                for (int i = 0; i < arguments.Length; i++) {
                    _parameters[i] = arguments[i.ToString()];
                }
            }
            else {
                _parameters = JsInstance.EMPTY;
            }


            // Executes the statements in 'that' and use _this as the target of the call
            visitor.ExecuteFunction(function, _this, _parameters);
            return visitor.Result;
            //visitor.CallFunction(function, _this, _parameters);

            //return visitor.Result;
        }
    }
}
