using Microsoft.CodeAnalysis;

namespace Raven.Analyzers.Shared
{
    internal static class MethodTranslatabilityHelper
    {
        /// <summary>
        /// Returns true when <paramref name="method"/> is likely non-translatable inside a
        /// RavenDB index or query expression — i.e. it is a user-defined method whose containing
        /// type is declared in the current compilation's source, rather than in a referenced
        /// assembly (BCL, Raven.Client, etc.).
        /// </summary>
        /// <param name="exemptObjectMethodOverrides">
        /// When true (the index Map/Reduce context), a user override of a System.Object virtual
        /// (ToString/Equals/GetHashCode) is treated as translatable because the server rebinds it onto
        /// its DynamicBlittableJson wrapper when the map is compiled. When false (the client-side query
        /// translation context), no such rebind exists, so those overrides are still flagged.
        /// </param>
        internal static bool IsLikelyNonTranslatable(IMethodSymbol? method, bool exemptObjectMethodOverrides)
        {
            if (method == null || method.ContainingType == null)
                return false;

            // Delegate invocations (Invoke on Func<>/Action<>) are not static calls — skip.
            if (method.MethodKind == MethodKind.DelegateInvoke)
                return false;

            // Compiler-synthesized members — a record's / value type's Equals, GetHashCode, ToString,
            // operator==, the copy ctor, etc. — are not user-authored helpers. RavenDB handles equality
            // and value semantics on such members, so they must not be flagged even though their
            // containing type is declared in source.
            if (method.IsImplicitlyDeclared)
                return false;

            // In an index Map/Reduce, user overrides of the System.Object virtuals — ToString(),
            // Equals(object), GetHashCode() — are rebound by the server onto its DynamicBlittableJson
            // wrapper when the map is compiled, so they work at deployment even though the override lives
            // in source. Exempt any method that (transitively) overrides a System.Object member. This is
            // gated to the index context: client-side LINQ-to-RQL query translation is a different
            // mechanism with no such rebind, so RVN010 keeps flagging these. Genuine user helpers are not
            // overrides of Object members and remain flagged either way.
            if (exemptObjectMethodOverrides && OverridesObjectMember(method))
                return false;

            // Flag only when the METHOD ITSELF is declared in source, i.e. a user-authored helper —
            // not when merely the containing type has a source location. A type that is partly
            // source-generated (records, regex/JSON source generators, MVVM toolkit, etc.) has an
            // in-source location on the type even for members that live in the generated half; keying
            // off the method's own locations avoids flagging a call into the generated part. Methods
            // from referenced assemblies (BCL, Raven.Client) have no source location and stay unflagged.
            foreach (Location location in method.Locations)
            {
                if (location.IsInSource)
                    return true;
            }

            return false;
        }

        /// <summary>
        /// Returns true when <paramref name="method"/> (transitively) overrides a member of
        /// System.Object — i.e. it is a user override of ToString(), Equals(object), or GetHashCode().
        /// Walking the OverriddenMethod chain to its ultimate base handles overrides declared through
        /// an intermediate user base class as well as direct overrides.
        /// </summary>
        private static bool OverridesObjectMember(IMethodSymbol method)
        {
            if (method.IsOverride == false)
                return false;

            IMethodSymbol current = method;
            while (current.OverriddenMethod != null)
                current = current.OverriddenMethod;

            return current.ContainingType?.SpecialType == SpecialType.System_Object;
        }
    }
}
