using System;

namespace Raven.Client.Util
{
    internal static class ValidationMethods
    {
        internal static void AssertNotNullOrEmpty<T>(T key, string keyName)
        {
            switch (key)
            {
                case null:
                    throw new ArgumentNullException(keyName);
                case string str when string.IsNullOrEmpty(str):
                    throw new ArgumentException($"{keyName} cannot be null or empty", keyName);
            }
        }
    }
}
