using System;
using System.Collections.Generic;
using System.Text;

namespace Jint.Native {
    [Serializable]
    public class JsException : Exception {
        public JsInstance Value { get; set; }

        public JsException() {
        }

        public JsException(JsInstance value) {
            Value = value;
            //if (value is JsDictionaryObject)
            //    ((JsDictionaryObject)value)["jintException"] = new JsClr(this);
        }

		public override string Message
		{
			get { return base.Message + " Error: " + Value; }
		}
    }
}
