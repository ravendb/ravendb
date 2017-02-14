// -----------------------------------------------------------------------
//  <copyright file="ReplicationClientConfiguration.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------

using Sparrow.Json.Parsing;

namespace Raven.Client.Documents.Replication
{
    public class ReplicationClientConfiguration
    {
        public DynamicJsonValue ToJson()
        {
            return new DynamicJsonValue();
        }
    }
}
