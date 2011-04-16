//-----------------------------------------------------------------------
// <copyright file="ExpirationReadTrigger.cs" company="Hibernating Rhinos LTD">
//     Copyright (c) Hibernating Rhinos LTD. All rights reserved.
// </copyright>
//-----------------------------------------------------------------------
using System;
using Newtonsoft.Json.Linq;
using Raven.Abstractions.Data;
using Raven.Database;
using Raven.Database.Plugins;
using Raven.Http;
using Raven.Json.Linq;

namespace Raven.Bundles.Expiration
{
    public class ExpirationReadTrigger : AbstractReadTrigger
    {
        public const string RavenExpirationDate = "Raven-Expiration-Date";

		public override ReadVetoResult AllowRead(string key, RavenJObject metadata, ReadOperation operation,
                                                 TransactionInformation transactionInformation)
        {
            if(metadata == null)
                return ReadVetoResult.Allowed;
            var property = metadata[RavenExpirationDate];
            if (property == null)
                return ReadVetoResult.Allowed;
            var dateTime = property.Value<DateTime>();
            if(dateTime > GetCurrentUtcDate())
                return ReadVetoResult.Allowed;
            return ReadVetoResult.Ignore;
        }

        public static Func<DateTime> GetCurrentUtcDate = () => DateTime.UtcNow;
    }
}
