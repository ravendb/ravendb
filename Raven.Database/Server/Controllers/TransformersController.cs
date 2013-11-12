using System;
using System.Net;
using System.Net.Http;
using System.Threading.Tasks;
using System.Web.Http;
using Raven.Abstractions.Indexing;
using Raven.Json.Linq;

namespace Raven.Database.Server.Controllers
{
	public class TransformersController : RavenApiController
	{
		[HttpGet]
		[Route("transformers/{*id}")]
		[Route("databases/{databaseName}/transformers/{*id}")]
		public HttpResponseMessage TransformerGet(string id)
		{
			var transformer = id;
			if (string.IsNullOrEmpty(transformer) == false && transformer != "/")
			{
				var transformerDefinition = Database.GetTransformerDefinition(transformer);
				if (transformerDefinition == null)
					return GetEmptyMessage(HttpStatusCode.NotFound);

				return GetMessageWithObject(new
				{
					Transformer = transformerDefinition,
				});
			}
			return GetEmptyMessage();
		}

		[HttpGet]
		[Route("transformers")]
		[Route("databases/{databaseName}/transformers")]
		public HttpResponseMessage TransformerGet()
		{
			var namesOnlyString = GetQueryStringValue("namesOnly");
			bool namesOnly;
			RavenJArray transformers;
			if (bool.TryParse(namesOnlyString, out namesOnly) && namesOnly)
				transformers = Database.GetTransformerNames(GetStart(), GetPageSize(Database.Configuration.MaxPageSize));
			else
				transformers = Database.GetTransformers(GetStart(), GetPageSize(Database.Configuration.MaxPageSize));

			return GetMessageWithObject(transformers);
		}

		[HttpPut]
		[Route("transformers/{*id}")]
		[Route("databases/{databaseName}/transformers/{*id}")]
		public async Task<HttpResponseMessage> TransformersPut(string id)
		{
			var transformer = id;
			var data = await ReadJsonObjectAsync<TransformerDefinition>();
			if (data == null || string.IsNullOrEmpty(data.TransformResults))
				return GetMessageWithString("Expected json document with 'TransformResults' property", HttpStatusCode.BadRequest);

			try
			{
				var transformerName = Database.PutTransform(transformer, data);
				return GetMessageWithObject(new { Transformer = transformerName }, HttpStatusCode.Created);
			}
			catch (Exception ex)
			{
				return GetMessageWithObject(new
				{
					ex.Message,
					Error = ex.ToString()
				}, HttpStatusCode.BadRequest);
			}
		}

		[HttpDelete]
		[Route("transformers/{*id}")]
		[Route("databases/{databaseName}/transformers/{*id}")]
		public HttpResponseMessage TransformersDelete(string id)
		{
			Database.DeleteTransfom(id);
			return GetEmptyMessage(HttpStatusCode.NoContent);
		}
	}
}
