using System;
using System.Collections.Generic;
using System.Text;

namespace Jint.Native
{
    public abstract class JsLiteral : JsObject, ILiteral {
        public JsLiteral() {
        }

        public JsLiteral(object value, JsObject prototype)
            : base(prototype) {
            this.value = value;
        }

        public JsLiteral(JsObject prototype)
            : base(prototype) {
        }

        public override void DefineOwnProperty(Descriptor currentDescriptor) {
            // Do nothing, you should not be able to define new properties
            // on literals
        }

        public override void DefineOwnProperty(string key, JsInstance value) {
            // Do nothing, you should not be able to define new properties
            // on literals
        }
    }
}
