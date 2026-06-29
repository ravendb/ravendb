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
        internal static bool IsLikelyNonTranslatable(IMethodSymbol? method)
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
    }
}
