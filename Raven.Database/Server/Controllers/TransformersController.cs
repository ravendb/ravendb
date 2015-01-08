using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
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
		[RavenRoute("transformers/replicate-all")]
		[RavenRoute("databases/{databaseName}/transformers/replicate-all")]
		public HttpResponseMessage TransformersReplicate()
		{
			HttpResponseMessage erroResponseMessage;
			var replicationDocument = GetReplicationDocument(out erroResponseMessage);
			if (replicationDocument == null)
				return erroResponseMessage;
			

			var httpRavenRequestFactory = new HttpRavenRequestFactory { RequestTimeoutInMs = Database.Configuration.Replication.ReplicationRequestTimeoutInMilliseconds };

			var enabledReplicationDestinations = replicationDocument.Destinations
																	.Where(dest => dest.Disabled == false && dest.SkipIndexReplication == false)
																	.ToList();

			if (enabledReplicationDestinations.Count == 0)
				return GetMessageWithString("Replication is configured, but no enabled destinations found.", HttpStatusCode.NotFound);

			var allTransformerDefinitions = Database.Transformers.Definitions;
			if (allTransformerDefinitions.Length == 0)
				return GetMessageWithString("No transformers to replicate. Nothing to do.. ", HttpStatusCode.NotFound);

			var replicationRequestTasks = new List<Task>(enabledReplicationDestinations.Count * allTransformerDefinitions.Length);

			var failedDestinations = new ConcurrentBag<string>();
			foreach (var definition in allTransformerDefinitions)
			{
				var clonedDefinition = definition.Clone();
				clonedDefinition.TransfomerId = 0;
				replicationRequestTasks.AddRange(
					enabledReplicationDestinations
									   .Select(destination =>
										   Task.Run(() =>
											   ReplicateTransformer(definition.Name, destination,
													RavenJObject.FromObject(clonedDefinition),
													failedDestinations,
													httpRavenRequestFactory))).ToList());
			}

			Task.WaitAll(replicationRequestTasks.ToArray());

			return GetMessageWithObject(new
			{
				TransformerCount = allTransformerDefinitions.Length,
				EnabledDestinationsCount = enabledReplicationDestinations.Count,
				SuccessfulReplicationCount = ((enabledReplicationDestinations.Count * allTransformerDefinitions.Length) - failedDestinations.Count),
				FailedDestinationUrls = failedDestinations
			});
		}


		[HttpPost]
		[RavenRoute("transformers/replicate/{*transformerName}")]
		[RavenRoute("databases/{databaseName}/transformers/replicate/{*transformerName}")]
		public HttpResponseMessage TransformersReplicate(string transformerName)
		{
			if (transformerName == null) 
				throw new ArgumentNullException("transformerName");

			HttpResponseMessage erroResponseMessage;
			var replicationDocument = GetReplicationDocument(out erroResponseMessage);
			if (replicationDocument == null)
				return erroResponseMessage;

			if (string.IsNullOrWhiteSpace(transformerName) || transformerName.StartsWith("/"))
				return GetMessageWithString(String.Format("Invalid transformer name! Received : '{0}'",transformerName), HttpStatusCode.NotFound);				

			var transformerDefinition = Database.Transformers.GetTransformerDefinition(transformerName);
			if (transformerDefinition == null)
				return GetEmptyMessage(HttpStatusCode.NotFound);

			var clonedTransformerDefinition = transformerDefinition.Clone();
			clonedTransformerDefinition.TransfomerId = 0;
			
			var serializedTransformerDefinition = RavenJObject.FromObject(clonedTransformerDefinition);
			var httpRavenRequestFactory = new HttpRavenRequestFactory { RequestTimeoutInMs = Database.Configuration.Replication.ReplicationRequestTimeoutInMilliseconds };

			var failedDestinations = new ConcurrentBag<string>();
			Parallel.ForEach(replicationDocument.Destinations.Where(x=>x.Disabled == false && x.SkipIndexReplication == false),
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

			//databases/{databaseName}/transformers/{*id}
			const string urlTemplate = "{0}/databases/{1}/transformers/{2}";
			if (Uri.IsWellFormedUriString(destination.Url, UriKind.RelativeOrAbsolute) == false)
			{
				const string error = "Invalid destination URL";
				failedDestinations.Add(destination.Url);
				Log.Error(error);
				return;
			}

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
