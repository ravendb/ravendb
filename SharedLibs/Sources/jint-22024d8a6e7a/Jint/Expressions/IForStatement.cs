using System;
using System.Collections.Generic;
using System.Text;

namespace Jint.Expressions {
    public interface IForStatement {
        Statement InitialisationStatement { get; set; }
        Statement Statement { get; set; }
    }
}
