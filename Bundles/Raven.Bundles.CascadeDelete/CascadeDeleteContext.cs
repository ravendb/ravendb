using System;
using System.Collections.Generic;
using Raven.Database.Data;


namespace Raven.Bundles.CascadeDelete
{

    public static class CascadeDeleteContext
    {
        [ThreadStatic]
        private static bool _currentlyInContext;

        [ThreadStatic]
        private static HashSet<string> _deletedDocuments = new HashSet<string>(StringComparer.InvariantCultureIgnoreCase);

        public static bool IsInCascadeDeleteContext
        {
            get
            {
                return _currentlyInContext;
            }
        }

        public static bool HasAlreadyDeletedDocument(string key)
        {
            return _deletedDocuments.Contains(key);
        }

        public static void AddDeletedDocument(string key)
        {
            _deletedDocuments.Add(key);
        }

        public static IDisposable Enter()
        {
            var oldCurrentlyInContext = _currentlyInContext;
            var oldDeletedDocuments = _deletedDocuments;
            _currentlyInContext = true;
            _deletedDocuments = new HashSet<string>(StringComparer.InvariantCultureIgnoreCase);
            return new DisposableAction(delegate
            {
                _currentlyInContext = oldCurrentlyInContext;
                _deletedDocuments = oldDeletedDocuments;
            });
        }
    }

}
