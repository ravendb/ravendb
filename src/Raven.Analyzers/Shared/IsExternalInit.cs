namespace System.Runtime.CompilerServices
{
    // Compiler shim required to use C# record/init-only members when targeting netstandard2.0,
    // which does not ship the IsExternalInit type. Compile-time only; emits no runtime dependency.
    internal static class IsExternalInit
    {
    }
}
