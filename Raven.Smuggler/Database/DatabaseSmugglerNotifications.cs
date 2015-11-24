using System;

using Raven.Smuggler.Common;

namespace Raven.Smuggler.Database
{
    public class DatabaseSmugglerNotifications : SmugglerNotifications
    {
        public EventHandler<string> OnDocumentRead = (sender, key) => { };

        public EventHandler<string> OnDocumentWrite = (sender, key) => { };

        public EventHandler<string> OnDocumentDeletionRead = (sender, key) => { };

        public EventHandler<string> OnDocumentDeletionWrite = (sender, key) => { };
    }
}
