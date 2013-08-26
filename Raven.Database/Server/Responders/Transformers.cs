using System;
using Raven.Abstractions.Exceptions;
using Raven.Abstractions.Indexing;
using Raven.Database.Extensions;
using Raven.Database.Server.Abstractions;
using Raven.Json.Linq;

namespace Raven.Database.Server.Responders
{
	public class Transformers : AbstractRequestResponder
	{
		public override string UrlPattern
		{
			get { return "^/transformers/?(.+)?$"; }
		}

		public override string[] SupportedVerbs
		{
			get { return new[] { "GET", "PUT", "DELETE" }; }
		}

		public override void Respond(IHttpContext context)
		{
			var match = urlMatcher.Match(context.GetRequestUrl());
			var transformer = match.Groups[1].Value;
			switch (context.Request.HttpMethod)
			{
				case "GET":
					if (string.IsNullOrEmpty(transformer) == false && transformer != "/")
					{
					    var transformerDefinition = Database.GetTransformerDefinition(transformer);
                        if (transformerDefinition == null)
                        {
                            context.SetStatusToNotFound();
                            return;
                        }

						context.WriteJson(new
						{
							Transformer = transformerDefinition,
						});
						break;
					}

					var namesOnlyString = context.Request.QueryString["namesOnly"];
					bool namesOnly;
					RavenJArray transformers;
					if (bool.TryParse(namesOnlyString, out namesOnly) && namesOnly)
						transformers = Database.GetTransformerNames(context.GetStart(), context.GetPageSize(Database.Configuration.MaxPageSize));
					else
						transformers = Database.GetTransformers(context.GetStart(), context.GetPageSize(Database.Configuration.MaxPageSize));
					context.WriteJson(transformers);
					break;
				case "PUT":
					HandlePut(context, transformer);
			        break;
				case "DELETE":
					context.SetStatusToDeleted();
					Database.DeleteTransfom(transformer);
					break;
			}
		}

	    private void HandlePut(IHttpContext context, string transformer)
	    {
	        var data = context.ReadJsonObject<TransformerDefinition>();
	        if (data == null || string.IsNullOrEmpty(data.TransformResults))
	        {
	            context.SetStatusToBadRequest();
	            context.Write("Expected json document with 'TransformResults' property");
	            return;
	        }

	        try
	        {
	            var transformerName = Database.PutTransform(transformer, data);
	            context.SetStatusToCreated("/transformers");
	            context.WriteJson(new {Transformer = transformerName});
	        }
            catch (Exception ex)
            {
                context.SetStatusToBadRequest();
                context.WriteJson(new
                {
                    Message = ex.Message,
                    Error = ex.ToString()
                });
            }
	    }
	}
}