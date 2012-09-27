using System;
using System.Collections.Generic;
using System.Text;
using Jint.Expressions;

namespace Jint.Native {
    [Serializable]
    public class JsObjectConstructor : JsConstructor {
        public JsObjectConstructor(IGlobal global, JsObject prototype, JsObject rootPrototype)
            : base(global) {
            Name = "Object";
            DefineOwnProperty(PROTOTYPE, rootPrototype, PropertyAttributes.DontEnum | PropertyAttributes.DontDelete | PropertyAttributes.ReadOnly);
        }

        public override void InitPrototype(IGlobal global) {
            var Prototype = PrototypeProperty;

            // we need to keep this becouse the prototype is passed to the consctrucor rather than created in it
            Prototype.DefineOwnProperty("constructor", this, PropertyAttributes.DontEnum);

            //Prototype.DefineOwnProperty("length", new PropertyDescriptor<JsDictionaryObject>(global, Prototype, "length", GetLengthImpl, SetLengthImpl));

            Prototype.DefineOwnProperty("toString", global.FunctionClass.New<JsDictionaryObject>(ToStringImpl), PropertyAttributes.DontEnum);
            Prototype.DefineOwnProperty("toLocaleString", global.FunctionClass.New<JsDictionaryObject>(ToStringImpl), PropertyAttributes.DontEnum);
            Prototype.DefineOwnProperty("valueOf", global.FunctionClass.New<JsDictionaryObject>(ValueOfImpl), PropertyAttributes.DontEnum);
            Prototype.DefineOwnProperty("hasOwnProperty", global.FunctionClass.New<JsDictionaryObject>(HasOwnPropertyImpl), PropertyAttributes.DontEnum);
            Prototype.DefineOwnProperty("isPrototypeOf", global.FunctionClass.New<JsDictionaryObject>(IsPrototypeOfImpl), PropertyAttributes.DontEnum);
            Prototype.DefineOwnProperty("propertyIsEnumerable", global.FunctionClass.New<JsDictionaryObject>(PropertyIsEnumerableImpl), PropertyAttributes.DontEnum);
            Prototype.DefineOwnProperty("getPrototypeOf", new JsFunctionWrapper(GetPrototypeOfImpl, global.FunctionClass.PrototypeProperty), PropertyAttributes.DontEnum);
            if (global.HasOption(Options.Ecmascript5)) {
                Prototype.DefineOwnProperty("defineProperty", new JsFunctionWrapper(DefineProperty, global.FunctionClass.PrototypeProperty), PropertyAttributes.DontEnum);
                Prototype.DefineOwnProperty("__lookupGetter__", global.FunctionClass.New<JsDictionaryObject>(GetGetFunction), PropertyAttributes.DontEnum);
                Prototype.DefineOwnProperty("__lookupSetter__", global.FunctionClass.New<JsDictionaryObject>(GetSetFunction), PropertyAttributes.DontEnum);
            }
        }

        /// <summary>
        /// Creates new JsObject, sets a [[Prototype]] to the Object.PrototypeProperty and a 'constructor' property to the specified function
        /// </summary>
        /// <param name="constructor">JsFunction which is used as a constructor</param>
        /// <returns>new object</returns>
        public JsObject New(JsFunction constructor) {
            JsObject obj = new JsObject(this.PrototypeProperty);
            obj.DefineOwnProperty(new ValueDescriptor(obj, CONSTRUCTOR, constructor) { Enumerable = false });
            return obj;
        }

        /// <summary>
        /// Creates new JsObject, sets a [[Prototype]] to the Prototype parameter and a 'constructor' property to the specified function.
        /// </summary>
        /// <param name="constructor">JsFunction which is used as a constructor</param>
        /// <param name="Prototype">JsObjetc which is used as a prototype</param>
        /// <returns>new object</returns>
        public JsObject New(JsFunction constructor, JsObject Prototype)
        {
            JsObject obj = new JsObject(Prototype);
            obj.DefineOwnProperty(new ValueDescriptor(obj, CONSTRUCTOR, constructor) { Enumerable = false });
            return obj;
        }

        /// <summary>
        /// Creates a new object which holds a specified value
        /// </summary>
        /// <param name="value">Value to store in the new object</param>
        /// <returns>new object</returns>
        public JsObject New(object value) {
            return new JsObject(value, this.PrototypeProperty);
        }

        public JsObject New(object value, JsObject prototype)
        {
            return new JsObject(value, prototype);
        }

        public JsObject New() {
            return New((object)null);
        }

        public JsObject New(JsObject prototype)
        {
            return new JsObject(prototype);
        }

        /// <summary>
        /// 15.2.2.1
        /// </summary>
        public override JsInstance Execute(IJintVisitor visitor, JsDictionaryObject that, JsInstance[] parameters) {
            if (parameters.Length > 0) {
                switch (parameters[0].Class) {
                    case JsInstance.CLASS_STRING: return Global.StringClass.New(parameters[0].ToString());
                    case JsInstance.CLASS_NUMBER: return Global.NumberClass.New(parameters[0].ToNumber());
                    case JsInstance.CLASS_BOOLEAN: return Global.BooleanClass.New(parameters[0].ToBoolean());
                    default:
                        return parameters[0];
                }
            }

            return New(this);
        }

        // 15.2.4.3 and 15.2.4.4
        public JsInstance ToStringImpl(JsDictionaryObject target, JsInstance[] parameters) {
            return Global.StringClass.New(String.Concat("[object ", target.Class, "]"));
        }

        // 15.2.4.4
        public JsInstance ValueOfImpl(JsDictionaryObject target, JsInstance[] parameters) {
            return target;
        }

        // 15.2.4.5
        public JsInstance HasOwnPropertyImpl(JsDictionaryObject target, JsInstance[] parameters) {
            return Global.BooleanClass.New(target.HasOwnProperty(parameters[0]));
        }

        // 15.2.4.6
        public JsInstance IsPrototypeOfImpl(JsDictionaryObject target, JsInstance[] parameters) {
            if (target.Class != JsInstance.CLASS_OBJECT) {
                return Global.BooleanClass.False;
            }

            while (true) {
                IsPrototypeOf(target);
                if (target == null) {
                    return Global.BooleanClass.True;
                }

                if (target == this) {
                    return Global.BooleanClass.True;
                }
            }
        }

        // 15.2.4.7
        public JsInstance PropertyIsEnumerableImpl(JsDictionaryObject target, JsInstance[] parameters) {
            if (!HasOwnProperty(parameters[0])) {
                return Global.BooleanClass.False;
            }

            var v = target[parameters[0]];

            return Global.BooleanClass.New((v.Attributes & PropertyAttributes.DontEnum) == PropertyAttributes.None);
        }

        /// <summary>
        /// 15.2.3.2
        /// </summary>
        /// <returns></returns>
        public JsInstance GetPrototypeOfImpl(JsInstance[] parameters) {
            if (parameters[0].Class != JsInstance.CLASS_OBJECT)
                throw new JsException(Global.TypeErrorClass.New());
            return ((parameters[0] as JsObject ?? JsUndefined.Instance)[JsFunction.CONSTRUCTOR] as JsObject ?? JsUndefined.Instance)[JsFunction.PROTOTYPE];
        }

        /// <summary>
        /// 15.2.3.6
        /// </summary>
        /// <param name="instance"></param>
        /// <param name="p"></param>
        /// <param name="currentDescriptor"></param>
        public JsInstance DefineProperty(JsInstance[] parameters) {
            JsInstance instance = parameters[0];

            if (!(instance is JsDictionaryObject))
                throw new JsException(Global.TypeErrorClass.New());

            string name = parameters[1].ToString();
            Descriptor desc = Descriptor.ToPropertyDesciptor(Global, (JsDictionaryObject)instance, name, parameters[2]);

            ((JsDictionaryObject)instance).DefineOwnProperty(desc);

            return instance;
        }
    }
}