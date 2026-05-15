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
            INamedTypeSymbol? current = type;

            while (current != null && current.SpecialType != SpecialType.System_Object)
            {
                foreach (ISymbol member in current.GetMembers())
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

                current = current.BaseType;
            }

            return names.ToImmutableHashSet();
        }
    }
}
