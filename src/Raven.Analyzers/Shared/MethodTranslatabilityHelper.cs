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

            // Only flag methods whose containing type has at least one source location,
            // meaning the type is defined in the user's own code, not in a referenced assembly.
            foreach (Location location in method.ContainingType.Locations)
            {
                if (location.IsInSource)
                    return true;
            }

            return false;
        }
    }
}
