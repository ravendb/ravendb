using System;
using System.Collections.Generic;
using System.Text;

namespace Jint {
    public interface ITypeResolver {
        Type ResolveType(string fullname);

    }
}
