using System;
using System.Reflection;
using XunitLogger;

namespace Tests.Infrastructure
{
    public static class ContextExtensions
    {
        private readonly static FieldInfo _exceptionField;

        static ContextExtensions()
        {
            _exceptionField = typeof(Context).GetField("Exception", BindingFlags.NonPublic | BindingFlags.Public | BindingFlags.Instance);
            if (_exceptionField == null)
                throw new InvalidOperationException("Could not extract exception field info from context");
        }

        public static Exception GetException(this Context context)
        {
            if (context == null)
                return null;

            return _exceptionField.GetValue(context) as Exception;
        }
    }
}
