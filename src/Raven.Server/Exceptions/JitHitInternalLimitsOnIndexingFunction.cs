using System;

namespace Raven.Server.Exceptions
{
    public class JitHitInternalLimitsOnIndexingFunction : CriticalIndexingException
    {
        internal const string ErrorMessage = "Indexing has failed due to JIT limits on CoreCRL (https://github.com/dotnet/coreclr/issues/14672). " +
                                             "It related to high number of fields in the index output. " +
                                             "Consider reducing the number of fields in the 'select' statement or splitting the index into multiple smaller ones " +
                                             "in order to bypass this issue";

        public JitHitInternalLimitsOnIndexingFunction(InvalidProgramException e)
            : base(ErrorMessage, e)
        {
        }
    }
}
