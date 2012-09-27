using System;
using System.Collections.Generic;
using System.Text;

namespace Jint.Native {
    [Serializable]
    public class JsArguments : JsObject {
        public const string CALLEE = "callee";

        protected ValueDescriptor calleeDescriptor;

        protected JsFunction Callee {
            get { return this[CALLEE] as JsFunction; }
            set { this[CALLEE] = value; }
        }

        public JsArguments(IGlobal global, JsFunction callee, JsInstance[] arguments)
            : base(global.ObjectClass.New())
        {            
            this.global = global;

            // Add the named parameters            
            for (int i = 0; i < arguments.Length ; i++)
                this.DefineOwnProperty(
                    new ValueDescriptor(this, i.ToString(), arguments[i]) { Enumerable = false }
                );

            length = arguments.Length;

            calleeDescriptor = new ValueDescriptor(this, CALLEE, callee) { Enumerable = false };
            DefineOwnProperty(calleeDescriptor);

            DefineOwnProperty(new PropertyDescriptor<JsArguments>(global, this, "length", GetLength) { Enumerable = false });
        }

        private int length;
        private IGlobal global;

        public override bool IsClr
        {
            get
            {
                return false;
            }
        }

        public override bool ToBoolean() {
            return false;
        }

        public override double ToNumber() {
            return Length;
        }

        /// <summary>
        /// The number of the actually passed arguments
        /// </summary>
        public override int Length {
            get {
                return length;
            }
            set {
                length = value;
            }
        }

        public override string Class {
            get { return CLASS_ARGUMENTS; }
        }

        public JsInstance GetLength(JsArguments target) {
            return global.NumberClass.New(target.length);
        }
    }
}
