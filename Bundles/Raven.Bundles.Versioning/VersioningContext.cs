using System;
using Raven.Database.Data;

namespace Raven.Bundles.Versioning
{
	public static class VersioningContext
    {
        [ThreadStatic] private static bool _currentlyInContext;

		public static bool IsInVersioningContext
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
