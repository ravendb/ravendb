using System;
using System.Collections.Generic;
using System.Text;
using System.Reflection;

namespace Jint {
    public interface IConstructorInvoker {
        ConstructorInfo Invoke(Type type, object[] parameters);
    }
}
