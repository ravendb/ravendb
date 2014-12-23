using System;
using System.Collections.Concurrent;
using System.Net;
using System.Net.Http;
using System.Threading.Tasks;
using System.Web.Http;
using JetBrains.Annotations;
using Raven.Abstractions.Connection;
using Raven.Abstractions.Data;
using Raven.Abstractions.Indexing;
using Raven.Abstractions.Logging;
using Raven.Abstractions.Replication;
using Raven.Database.Server.WebApi.Attributes;
using Raven.Json.Linq;

namespace Raven.Database.Server.Controllers
{
	public class TransformersController : RavenDbApiController
	{
		[HttpGet]
		[RavenRoute("transformers/{*id}")]
		[RavenRoute("databases/{databaseName}/transformers/{*id}")]
		public HttpResponseMessage TransformerGet(string id)
		{
			var transformer = id;
			if (string.IsNullOrEmpty(transformer) == false && transformer != "/")
			{
				var transformerDefinition = Database.Transformers.GetTransformerDefinition(transformer);
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
		[RavenRoute("transformers")]
		[RavenRoute("databases/{databaseName}/transformers")]
		public HttpResponseMessage TransformerGet()
		{
			var namesOnlyString = GetQueryStringValue("namesOnly");
			bool namesOnly;
			RavenJArray transformers;
			if (bool.TryParse(namesOnlyString, out namesOnly) && namesOnly)
				transformers = Database.Transformers.GetTransformerNames(GetStart(), GetPageSize(Database.Configuration.MaxPageSize));
			else
				transformers = Database.Transformers.GetTransformers(GetStart(), GetPageSize(Database.Configuration.MaxPageSize));

			return GetMessageWithObject(transformers);
		}

		[HttpPut]
		[RavenRoute("transformers/{*id}")]
		[RavenRoute("databases/{databaseName}/transformers/{*id}")]
		public async Task<HttpResponseMessage> TransformersPut(string id)
		{
			var transformer = id;
			var data = await ReadJsonObjectAsync<TransformerDefinition>();
			if (data == null || string.IsNullOrEmpty(data.TransformResults))
				return GetMessageWithString("Expected json document with 'TransformResults' property", HttpStatusCode.BadRequest);

			try
			{
				var transformerName = Database.Transformers.PutTransform(transformer, data);
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
		[RavenRoute("transformers/{*id}")]
		[RavenRoute("databases/{databaseName}/transformers/{*id}")]
		public HttpResponseMessage TransformersDelete(string id)
		{
			Database.Transformers.DeleteTransform(id);
			return GetEmptyMessage(HttpStatusCode.NoContent);
		}

		[HttpPost]
		[Route("transformers/replicate/{*transformerName}")]
		[Route("databases/{databaseName}/transformers/replicate/{*transformerName}")]
		public HttpResponseMessage TransformersReplicate(string transformerName)
		{
			if (transformerName == null) 
				throw new ArgumentNullException("transformerName");

			HttpResponseMessage erroResponseMessage;
			var replicationDocument = GetReplicationDocument(out erroResponseMessage);
			if (replicationDocument == null)
				return erroResponseMessage;

			if (string.IsNullOrWhiteSpace(transformerName) == false && transformerName != "/")
				return GetMessageWithString("Invalid transformer name",HttpStatusCode.NotFound);				

			var transformerDefinition = Database.Transformers.GetTransformerDefinition(transformerName);
			if (transformerDefinition == null)
				return GetEmptyMessage(HttpStatusCode.NotFound);

			var serializedTransformerDefinition = RavenJObject.FromObject(transformerDefinition);
			var httpRavenRequestFactory = new HttpRavenRequestFactory { RequestTimeoutInMs = Database.Configuration.Replication.ReplicationRequestTimeoutInMilliseconds };

			var failedDestinations = new ConcurrentBag<string>();
			Parallel.ForEach(replicationDocument.Destinations,
				destination => ReplicateTransformer(transformerName, destination, serializedTransformerDefinition, failedDestinations, httpRavenRequestFactory));

			return GetMessageWithObject(new
			{
				SuccessfulReplicationCount = (replicationDocument.Destinations.Count - failedDestinations.Count),
				FailedDestinationUrls = failedDestinations
			});			
		}

		private void ReplicateTransformer(string transformerName, ReplicationDestination destination, RavenJObject transformerDefinition, ConcurrentBag<string> failedDestinations, HttpRavenRequestFactory httpRavenRequestFactory)
		{
			var connectionOptions = new RavenConnectionStringOptions
			{
				ApiKey = destination.ApiKey,
				Url = destination.Url,
				DefaultDatabase = destination.Database
			};

			if (!String.IsNullOrWhiteSpace(destination.Username) &&
				!String.IsNullOrWhiteSpace(destination.Password))
			{
				connectionOptions.Credentials = new NetworkCredential(destination.Username, destination.Password, destination.Domain ?? string.Empty);
			}

			var urlTemplate = "{0}/databases/{1}/indexes/{2}";
			if (destination.Url.Contains("://") == false)
				urlTemplate = "//" + urlTemplate;
			var operationUrl = string.Format(urlTemplate, destination.Url, destination.Database, Uri.EscapeUriString(transformerName));
			var replicationRequest = httpRavenRequestFactory.Create(operationUrl, "PUT", connectionOptions);
			replicationRequest.Write(transformerDefinition);

			try
			{
				replicationRequest.ExecuteRequest();
			}
			catch (Exception e)
			{
				Log.ErrorException("failed to replicate index to: " + destination.Url, e);
				failedDestinations.Add(destination.Url);
			}
		}
	}
}
