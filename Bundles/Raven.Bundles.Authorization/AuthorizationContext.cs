using System;
using Raven.Database.Data;

namespace Raven.Bundles.Authorization
{
    public static class AuthorizationContext
    {
        [ThreadStatic] private static bool _currentlyInContext;

        public static bool IsInAuthorizationContext
        {
            get
            {
                return _currentlyInContext;
            }
        }

        public static IDisposable Enter()
        {
            var old = _currentlyInContext;
            _currentlyInContext = true;
            return new DisposableAction(() => _currentlyInContext = old);
        }
    }
}
