// -----------------------------------------------------------------------
//  <copyright file="DebugChanges.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------
using Raven.Database.Server.Abstractions;
using Raven.Database.Extensions;

namespace Raven.Database.Server.Responders.Debugging
{
    public class DebugChanges : AbstractRequestResponder
    {
        public override string UrlPattern
        {
            get { return @"^/debug/changes"; }
        }

        public override string[] SupportedVerbs
        {
            get { return new[] {"GET"}; }
        }

        public override void Respond(IHttpContext context)
        {
            context.WriteJson(Database.TransportState.DebugStatuses);
        }
    }
}