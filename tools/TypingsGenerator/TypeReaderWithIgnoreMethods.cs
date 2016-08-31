using System.Collections.Generic;
using System.Linq;
using System.Reflection;
using TypeScripter.Readers;

namespace TypingsGenerator
{
    public class TypeReaderWithIgnoreMethods : TypeReader
    {
        public override IEnumerable<MethodInfo> GetMethods(TypeInfo type)
        {
            return Enumerable.Empty<MethodInfo>();
        }
    }
}