using System;
using System.Collections.Generic;
using TypeScripter;
using TypeScripter.TypeScript;

namespace TypingsGenerator
{
    public class CustomScripter : Scripter
    {
        protected override TsName GetName(Type type)
        {
            if (type == null)
                throw new ArgumentNullException(nameof(type));

            var tsName = base.GetName(type);

            if (type.IsNested)
            {
                // For nested types, module name should include the full declaring-type chain.
                return new TsName(tsName.Name, GetDeclaringTypeModuleName(type));
            }

            return tsName;
        }

        private static string GetDeclaringTypeModuleName(Type type)
        {
            var declaringNames = new Stack<string>();
            for (var declaringType = type.DeclaringType; declaringType != null; declaringType = declaringType.DeclaringType)
            {
                declaringNames.Push(RemoveGenericArity(declaringType.Name));
            }

            var nestedPath = string.Join(".", declaringNames);
            if (string.IsNullOrEmpty(type.Namespace))
                return nestedPath;

            return string.IsNullOrEmpty(nestedPath) ? type.Namespace : $"{type.Namespace}.{nestedPath}";
        }

        private static string RemoveGenericArity(string typeName)
        {
            var genericMarkerIndex = typeName.IndexOf('`');
            return genericMarkerIndex >= 0 ? typeName.Substring(0, genericMarkerIndex) : typeName;
        }
    }
}
