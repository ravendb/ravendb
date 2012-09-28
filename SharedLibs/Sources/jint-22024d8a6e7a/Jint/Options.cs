using System;
using System.Collections.Generic;
using System.Text;

namespace Jint {
    [Flags]
    public enum Options {
        Strict = 1,
        Ecmascript3 = 2,
        Ecmascript5 = 4
    }
}
