using System;
using System.Collections.Generic;
using System.Text;

namespace Jint.Native
{
    class NativeTypeConstructor: NativeConstructor
    {
        protected NativeTypeConstructor(IGlobal global, JsObject typePrototype)
            : base(typeof(Type), global,typePrototype,typePrototype)
        {
            // redefine prototype
            DefineOwnProperty(PROTOTYPE, typePrototype);
        }

        /// <summary>
        /// A static fuction for creating a constructor for <c>System.Type</c>
        /// </summary>
        /// <remarks>It also creates and initializes [[prototype]] and 'prototype' property to
        /// the same function object.</remarks>
        /// <param name="global">Global object</param>
        /// <returns>A js constructor function</returns>
        public static NativeTypeConstructor CreateNativeTypeConstructor(IGlobal global)
        {
            if (global == null)
                throw new ArgumentNullException("global");

            JsObject proto = global.FunctionClass.New();
            var inst = new NativeTypeConstructor(global, proto);
            inst.InitPrototype(global);
            inst.SetupNativeProperties(inst);
            return inst;
        }

        public override JsInstance Wrap<T>(T value)
        {
            if (value == null)
                throw new ArgumentNullException("value");

            if (value is Type)
            {
                NativeConstructor res;
                res = new NativeConstructor(value as Type, Global,null,PrototypeProperty);
                res.InitPrototype(Global);
                SetupNativeProperties(res);
                return res;
            }
            else
                throw new JintException("Attempt to wrap '" + value.GetType().FullName + "' with '" + typeof(Type).FullName + "'");
        }

        public JsInstance WrapSpecialType(Type value, JsObject prototypePropertyPrototype)
        {
            if (value == null)
                throw new ArgumentNullException("value");

            
            NativeConstructor res;
            res = new NativeConstructor(value as Type, Global, prototypePropertyPrototype, PrototypeProperty);
            res.InitPrototype(Global);
            SetupNativeProperties(res);
            return res;
        }
    }
}
