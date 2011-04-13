//-----------------------------------------------------------------------
// <copyright file="LinearQueryResponder.cs" company="Hibernating Rhinos LTD">
//     Copyright (c) Hibernating Rhinos LTD. All rights reserved.
// </copyright>
//-----------------------------------------------------------------------
using Raven.Abstractions.Extensions;
using Raven.Database.Data;
using Raven.Database.Json;
using Raven.Database.Queries;
using Raven.Http.Abstractions;
using Raven.Http.Extensions;

namespace Raven.Database.Server.Responders
{
    public class LinearQueryResponder : RequestResponder
    {
        public override string UrlPattern
        {
            get { return "^/linearQuery$"; }
        }

        public override string[] SupportedVerbs
        {
            get { return new[] {"POST"}; }
        }

        public override void Respond(IHttpContext context)
        {
            var query = context.ReadJson().JsonDeserialization<LinearQuery>();
            var linearQueryResults = Database.ExecuteQueryUsingLinearSearch(query);
            context.WriteJson(linearQueryResults);
        }
    }
}
