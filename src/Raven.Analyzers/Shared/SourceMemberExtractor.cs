using System.Collections.Immutable;
using System.Collections.Generic;
using Microsoft.CodeAnalysis;

namespace Raven.Analyzers.Shared
{
    /// <summary>
    /// Extracts non-static property and field names from a type, walking the base-type chain.
    /// Includes <c>public</c> members always, and <c>internal</c> members when the type has source
    /// locations in the current compilation (i.e. it is defined in the user's own project rather
    /// than a referenced assembly).  Mirrors the reflection used at runtime by
    /// <c>ReflectionUtil.GetPropertiesAndFieldsFor&lt;T&gt;</c>.
    /// </summary>
    internal static class SourceMemberExtractor
    {
        public static ImmutableHashSet<string> GetPublicMembers(INamedTypeSymbol type)
        {
            bool includeInternal = IsInSourceCompilation(type);
            var names = new HashSet<string>(System.StringComparer.Ordinal);
            INamedTypeSymbol? current = type;

            while (current != null && current.SpecialType != SpecialType.System_Object)
            {
                foreach (ISymbol member in current.GetMembers())
                {
                    if (member.IsStatic)
                        continue;

                    bool accessible = member.DeclaredAccessibility == Accessibility.Public
                        || (includeInternal && member.DeclaredAccessibility == Accessibility.Internal);
                    if (!accessible)
                        continue;

                    if (member is IPropertySymbol prop && !prop.IsIndexer)
                        names.Add(prop.Name);
                    else if (member is IFieldSymbol)
                        names.Add(member.Name);
                }

                current = current.BaseType;
            }

            return names.ToImmutableHashSet();
        }

        private static bool IsInSourceCompilation(INamedTypeSymbol type)
        {
            foreach (Location location in type.Locations)
            {
                if (location.IsInSource)
                    return true;
            }
            return false;
        }
    }
}
