using System;
using Raven.Database.Plugins;
using Raven.Http;
using Raven.Database.Data;
using Raven.Json.Linq;

namespace Raven.Tests.Triggers.Bugs
{
    public class AuditTrigger : AbstractPutTrigger
    {
		public override void OnPut(string key, RavenJObject document, RavenJObject metadata, Raven.Http.TransactionInformation transactionInformation)
        {
            if (AuditContext.IsInAuditContext)
                return;

            using (AuditContext.Enter())
            {
                if (metadata.Value<string>("Raven-Entity-Name") == "People")
                {
                    if (metadata["CreatedByPersonId"] == null)
                    {
                        metadata["CreatedByPersonId"] = CurrentOperationContext.Headers.Value["CurrentUserPersonId"];
                        metadata["CreatedDate"] = new DateTime(2011,02,19,15,00,00);
                    }
                    else
                    {
                        metadata["LastUpdatedPersonId"] = CurrentOperationContext.Headers.Value["CurrentUserPersonId"];
						metadata["LastUpdatedDate"] = new DateTime(2011, 02, 19, 15, 00, 00);
                    }
                }
            }
        }
    }

    public static class AuditContext
    {
        [ThreadStatic]
        private static bool _currentlyInContext;

        public static bool IsInAuditContext
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
