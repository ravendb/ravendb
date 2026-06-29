using System.Collections.Immutable;
using System.Collections.Generic;
using Microsoft.CodeAnalysis;

namespace Raven.Analyzers.Shared
{
    /// <summary>
    /// Extracts public non-static property and field names from a type, walking the base-type chain.
    /// Mirrors <c>ReflectionUtil.BindingFlagsConstants.QueryingFields</c> (<c>Instance | Public</c>)
    /// used at runtime by <c>ProjectInto</c> / <c>SelectFields</c>.
    /// </summary>
    internal static class SourceMemberExtractor
    {
        public static ImmutableHashSet<string> GetPublicMembers(INamedTypeSymbol type)
        {
            var names = new HashSet<string>(System.StringComparer.Ordinal);

            // Walk the base-type chain (classes). Interfaces have no base type, so their inherited
            // members live on the interfaces they extend, handled separately below.
            for (INamedTypeSymbol? current = type;
                 current != null && current.SpecialType != SpecialType.System_Object;
                 current = current.BaseType)
            {
                AddPublicInstanceMembers(current, names);
            }

            // For an interface projection type (e.g. ProjectInto<IMyDto>()), include members inherited
            // from base interfaces — they are retrievable just like the interface's own members but do
            // not appear on a (non-existent) base type.
            if (type.TypeKind == TypeKind.Interface)
            {
                foreach (INamedTypeSymbol baseInterface in type.AllInterfaces)
                    AddPublicInstanceMembers(baseInterface, names);
            }

            return names.ToImmutableHashSet();
        }

        private static void AddPublicInstanceMembers(INamedTypeSymbol type, HashSet<string> names)
        {
            foreach (ISymbol member in type.GetMembers())
            {
                if (member.IsStatic)
                    continue;

                if (member.DeclaredAccessibility != Accessibility.Public)
                    continue;

                if (member is IPropertySymbol prop && !prop.IsIndexer)
                    names.Add(prop.Name);
                else if (member is IFieldSymbol)
                    names.Add(member.Name);
            }
        }
    }
}
