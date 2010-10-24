using System;
using Raven.Database.Server.Abstractions;

namespace Raven.Database.Server.Responders
{
	public class Statics : RequestResponder
	{
		public override string UrlPattern
		{
			get { return "^/static/?$"; }
		}

		public override string[] SupportedVerbs
		{
			get { return new[] { "GET", }; }
		}

		public override void Respond(IHttpContext context)
		{
			var array = Database.GetAttachments(context.GetStart(), 
			                                   context.GetPageSize(Database.Configuration.MaxPageSize),
			                                   context.GetEtagFromQueryString());
			context.WriteJson(array);
		}
	}

    public abstract class RequestResponder : AbstractRequestResponder
    {
        public DocumentDatabase Database
        {
            get
            {
                return (DocumentDatabase)ResourceStore;
            }
        }
    }
}
