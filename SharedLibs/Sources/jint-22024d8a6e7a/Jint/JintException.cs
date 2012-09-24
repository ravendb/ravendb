using System;
using System.Collections.Generic;
using System.Text;

namespace Jint {
    [Serializable]
    public class JintException : Exception {
        public JintException()
            : base() {
        }

        public JintException(string message)
            : base(message) {
        }

        public JintException(string message, Exception innerException)
            : base(message, innerException) {
        }
    }
}
