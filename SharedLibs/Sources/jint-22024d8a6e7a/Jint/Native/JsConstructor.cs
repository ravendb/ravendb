using System;
using System.Collections.Generic;
using System.Text;

namespace Jint.Native {
    [Serializable]
    public abstract class JsConstructor : JsFunction {
        /// <summary>
        /// Stores Global object used for creating this function.
        /// This property may be used in the InitProtype method.
        /// </summary>
        public IGlobal Global { get; set; }

        /// <summary>
        /// Constructs JsContructor, setting [[Prototype]] property to global.FunctionClass.PrototypeProperty
        /// </summary>
        /// <param name="global">Global</param>
        public JsConstructor(IGlobal global)
            : base(global) {
            Global = global;
        }

        /// <summary>
        /// Special form of the contructor used when constructin JsFunctionConstructor
        /// </summary>
        /// <remarks>This constructor is called when the global.FunctionClass isn't set yet.</remarks>
        /// <param name="global">Global</param>
        /// <param name="prototype">Prototype</param>
        protected JsConstructor(IGlobal global, JsObject prototype)
            : base(prototype) {
            Global = global;
        }

        public abstract void InitPrototype(IGlobal global);

        /// <summary>
        /// This method is used to wrap an native value with a js object of the specified type.
        /// </summary>
        /// <remarks>
        /// This method creates a new apropriate js object and stores
        /// </remarks>
        /// <typeparam name="T">A type of a native value to wrap</typeparam>
        /// <param name="value">A native value to wrap</param>
        /// <returns>A js instance</returns>
        public virtual JsInstance Wrap<T>(T value)
        {
            return new JsObject(value,PrototypeProperty);
        }

        public override string GetBody()
        {
            return "[native ctor]";
        }
    }
}
