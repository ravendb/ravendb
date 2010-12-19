using System;
using Raven.Http.Abstractions;
using Raven.Http.Extensions;

namespace Raven.Database.Server.Responders
{
    public class BuildVersion :RequestResponder
    {
        public override string UrlPattern
        {
            get { return "^/build/version$"; }
        }

        public override string[] SupportedVerbs
        {
            get { return new []{"GET"}; }
        }

        public override void Respond(IHttpContext context)
        {
            context.WriteJson(new
            {
                DocumentDatabase.ProductVersion,
                DocumentDatabase.BuildVersion
            });
        }
    }
}