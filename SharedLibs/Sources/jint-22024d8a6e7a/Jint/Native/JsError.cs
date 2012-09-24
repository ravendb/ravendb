using System;

namespace Jint.Native {
    [Serializable]
    public class JsError : JsObject {
        private string message { get { return this["message"].ToString(); } set { this["message"] = global.StringClass.New(value); } }

        public override bool IsClr
        {
            get
            {
                return false;
            }
        }

        public override object Value {
            get {
                return message;
            }
        }

        private IGlobal global;

        public JsError(IGlobal global)
            : this(global, string.Empty) {
        }

        public JsError(IGlobal global, string message)
            : base(global.ErrorClass.PrototypeProperty) {
            this.global = global;
            this.message = message;
        }

        public override string Class {
            get { return CLASS_ERROR; }
        }

        public override string ToString() {
            return Value.ToString();
        }
    }
}
