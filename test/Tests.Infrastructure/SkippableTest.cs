using System;

namespace FastTests
{
    // In xUnit v3, dynamic skipping is built-in.
    // Use Assert.Skip("reason") instead of throwing SkipTestException.
    // This class is kept for backward compatibility.
    public class SkipTestException : Exception
    {
        public SkipTestException(string reason)
            : base(reason) { }
    }
}
