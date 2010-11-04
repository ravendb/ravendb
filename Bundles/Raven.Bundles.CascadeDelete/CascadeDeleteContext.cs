using System;
using System.Collections.Generic;
using System.Threading;
using Raven.Database.Data;


namespace Raven.Bundles.CascadeDelete
{

    public static class CascadeDeleteContext
    {
        private static ThreadLocal<bool> _currentlyInContext = new ThreadLocal<bool>();

        private static readonly ThreadLocal<HashSet<string>> _deletedDocuments =
            new ThreadLocal<HashSet<string>>(() => new HashSet<string>(StringComparer.InvariantCultureIgnoreCase));

        public static bool IsInCascadeDeleteContext
        {
            get
            {
                return _currentlyInContext.Value;
            }
        }

        public static bool HasAlreadyDeletedDocument(string key)
        {
            return _deletedDocuments.Value.Contains(key);
        }

        public static void AddDeletedDocument(string key)
        {
            _deletedDocuments.Value.Add(key);
        }

        public static IDisposable Enter()
        {
            var oldCurrentlyInContext = _currentlyInContext.Value;
            var oldDeletedDocuments = _deletedDocuments.Value;
            _currentlyInContext.Value = true;
            _deletedDocuments.Value = new HashSet<string>(StringComparer.InvariantCultureIgnoreCase);
            return new DisposableAction(delegate
            {
                _currentlyInContext.Value = oldCurrentlyInContext;
                _deletedDocuments.Value = oldDeletedDocuments;
            });
        }
    }

}
