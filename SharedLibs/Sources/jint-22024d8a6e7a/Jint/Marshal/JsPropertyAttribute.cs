using System;
using System.Collections.Generic;
using System.Text;

namespace Jint.Marshal {
    [AttributeUsage(AttributeTargets.Property | AttributeTargets.Field)]
    public class JsPropertyAttribute : Attribute {
        public JsPropertyAttribute(string reflectedName) {
        }

        public JsPropertyAttribute() {
        }
    }
}
