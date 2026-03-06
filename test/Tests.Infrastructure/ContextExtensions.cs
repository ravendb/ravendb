using System;
using Xunit;

namespace Tests.Infrastructure
{
    public static class ContextExtensions
    {
        public static Exception GetException(this TestResultState state)
        {
            if (state == null || state.ExceptionMessages == null || state.ExceptionMessages.Length == 0)
                return null;

            var exceptionType = state.ExceptionTypes?.Length > 0 ? state.ExceptionTypes[0] : "Exception";
            var message = string.Join(Environment.NewLine, state.ExceptionMessages);
            return new Exception($"[{exceptionType}] {message}");
        }
    }
}
