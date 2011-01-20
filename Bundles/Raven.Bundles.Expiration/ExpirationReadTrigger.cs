//-----------------------------------------------------------------------
// <copyright file="ExpirationReadTrigger.cs" company="Hibernating Rhinos LTD">
//     Copyright (c) Hibernating Rhinos LTD. All rights reserved.
// </copyright>
//-----------------------------------------------------------------------
using System;
using Newtonsoft.Json.Linq;
using Raven.Database;
using Raven.Database.Plugins;
using Raven.Http;

namespace Raven.Bundles.Expiration
{
    public class ExpirationReadTrigger : AbstractReadTrigger
    {
        public const string RavenExpirationDate = "Raven-Expiration-Date"; 

        public override ReadVetoResult AllowRead(string key, JObject document, JObject metadata, ReadOperation operation,
                                                 TransactionInformation transactionInformation)
        {
            if(metadata == null)
                return ReadVetoResult.Allowed;
            var property = metadata.Property(RavenExpirationDate);
            if (property == null)
                return ReadVetoResult.Allowed;
            var dateTime = property.Value.Value<DateTime>();
            if(dateTime > GetCurrentUtcDate())
                return ReadVetoResult.Allowed;
            return ReadVetoResult.Ignore;
        }

        public static Func<DateTime> GetCurrentUtcDate = () => DateTime.UtcNow;
    }
}
