using System;
using System.Collections.Generic;
using System.Collections.Specialized;
using System.IO;
using System.Linq;
using System.Net;
using System.Net.Http;
using System.Text;
using System.Threading.Tasks;
using System.Web.Http;
using Raven.Abstractions.Logging;
using Raven.Client.RavenFS;
using Raven.Database.Server.RavenFS.Util;
using Raven.Imports.Newtonsoft.Json;
using NameValueCollectionJsonConverter = Raven.Client.RavenFS.NameValueCollectionJsonConverter;

namespace Raven.Database.Server.RavenFS.Controllers
{
	public class ConfigController : RavenFsApiController
	{
		private static readonly ILog Log = LogManager.GetCurrentClassLogger();

		[HttpGet]
		[Route("ravenfs/{fileSystemName}/config")]
		public string[] Get()
		{
			string[] names = null;
			Storage.Batch(accessor => { names = accessor.GetConfigNames(Paging.Start, Paging.PageSize).ToArray(); });
			return names;
		}

		[HttpGet]
        [Route("ravenfs/{fileSystemName}/config")]
		public HttpResponseMessage Get(string name)
		{
			try
			{
				NameValueCollection nameValueCollection = null;
				Storage.Batch(accessor => { nameValueCollection = accessor.GetConfig(name); });
				return Request.CreateResponse(HttpStatusCode.OK, nameValueCollection);
			}
			catch (FileNotFoundException)
			{
				return Request.CreateResponse(HttpStatusCode.NotFound);
			}
		}

		[HttpGet]
        [Route("ravenfs/{fileSystemName}/config/search")]
		public ConfigSearchResults ConfigNamesStartingWith(string prefix)
		{
			if (prefix == null)
				prefix = "";
			ConfigSearchResults results = null;
			Storage.Batch(accessor =>
			{
				int totalResults;
				var names = accessor.GetConfigNamesStartingWithPrefix(prefix, Paging.Start, Paging.PageSize,
																	  out totalResults);

				results = new ConfigSearchResults
				{
					ConfigNames = names,
					PageSize = Paging.PageSize,
					Start = Paging.Start,
					TotalCount = totalResults
				};
			});

			return results;
		}

		[HttpPut]
        [Route("ravenfs/{fileSystemName}/config")]
		public async Task<HttpResponseMessage> Put(string name)
		{
			var jsonSerializer = new JsonSerializer
			{
				Converters =
						                     {
							                     new NameValueCollectionJsonConverter()
						                     }
			};
			var contentStream = await Request.Content.ReadAsStreamAsync();

			var nameValueCollection =
				jsonSerializer.Deserialize<NameValueCollection>(new JsonTextReader(new StreamReader(contentStream)));

			ConcurrencyAwareExecutor.Execute(() => Storage.Batch(accessor => accessor.SetConfig(name, nameValueCollection)),
											 ConcurrencyResponseException);

			Publisher.Publish(new ConfigChange { Name = name, Action = ConfigChangeAction.Set });

			Log.Debug("Config '{0}' was inserted", name);

			return new HttpResponseMessage(HttpStatusCode.Created);
		}

		[HttpDelete]
        [Route("ravenfs/{fileSystemName}/config")]
		public HttpResponseMessage Delete(string name)
		{
			ConcurrencyAwareExecutor.Execute(() => Storage.Batch(accessor => accessor.DeleteConfig(name)),
											 ConcurrencyResponseException);

			Publisher.Publish(new ConfigChange { Name = name, Action = ConfigChangeAction.Delete });

			Log.Debug("Config '{0}' was deleted", name);
			return new HttpResponseMessage(HttpStatusCode.NoContent);
		}
	}
}