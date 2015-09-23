using System.Collections.Generic;
using System.Linq;
using System.Net;
using System.Net.Http;
using System.Text.RegularExpressions;
using System.Threading.Tasks;
using System.Web.Http;
using Raven.Abstractions.FileSystem;
using Raven.Abstractions.FileSystem.Notifications;
using Raven.Abstractions.Logging;
using Raven.Database.Extensions;
using Raven.Database.Server.WebApi.Attributes;
using Raven.Json.Linq;

namespace Raven.Database.FileSystem.Controllers
{
	public class ConfigController : BaseFileSystemApiController
	{
		private static new readonly ILog Log = LogManager.GetCurrentClassLogger();

		[HttpGet]
		[RavenRoute("fs/{fileSystemName}/config")]
        public HttpResponseMessage Get()
		{
			string[] names = null;
			Storage.Batch(accessor => { names = accessor.GetConfigNames(Paging.Start, Paging.PageSize).ToArray(); });

            return GetMessageWithObject(names)
                       .WithNoCache();
		}

		[HttpGet]
        [RavenRoute("fs/{fileSystemName}/config")]
        public HttpResponseMessage Get(string name)
		{
            RavenJObject config = null;

            Storage.Batch(accessor => 
            { 
                if ( accessor.ConfigExists(name))
                    config = accessor.GetConfig(name); 
            });

            HttpResponseMessage response = config != null ? GetMessageWithObject(config) 
                                                          : GetEmptyMessage(HttpStatusCode.NotFound);

            return response.WithNoCache();
		}

        [HttpGet]
        [RavenRoute("fs/{fileSystemName}/config/non-generated")]
        public HttpResponseMessage NonGeneratedConfigNames()
        {
			var searchPattern = new Regex("^(sync|deleteOp|raven\\/synchronization\\/sources|conflicted|renameOp)", RegexOptions.IgnoreCase);
            
			List<string> configs = null;
			Storage.Batch(accessor =>
			{
				configs = accessor
					.GetConfigNames(Paging.Start, int.MaxValue)
					.Where(config => !searchPattern.IsMatch(config))
					.Take(Paging.PageSize)
					.ToList();
			});

			return GetMessageWithObject(configs)
                       .WithNoCache();
        }

		[HttpGet]
        [RavenRoute("fs/{fileSystemName}/config/search")]
        public HttpResponseMessage ConfigNamesStartingWith(string prefix)
		{
			if (prefix == null)
				prefix = "";
			ConfigurationSearchResults results = null;
			Storage.Batch(accessor =>
			{
				int totalResults;
				var names = accessor.GetConfigNamesStartingWithPrefix(prefix, Paging.Start, Paging.PageSize,
																	  out totalResults);

				results = new ConfigurationSearchResults
				{
					ConfigNames = names,
					PageSize = Paging.PageSize,
					Start = Paging.Start,
					TotalCount = totalResults
				};
			});

            return GetMessageWithObject(results)
                       .WithNoCache();
		}

		[HttpPut]
        [RavenRoute("fs/{fileSystemName}/config")]
		public async Task<HttpResponseMessage> Put(string name)
		{
			var json = await ReadJsonAsync().ConfigureAwait(false);

			Storage.Batch(accessor => accessor.SetConfig(name, json));

			Publisher.Publish(new ConfigurationChangeNotification { Name = name, Action = ConfigurationChangeAction.Set });

			if (Log.IsDebugEnabled)
				Log.Debug("Config '{0}' was inserted", name);

			return GetMessageWithObject(json, HttpStatusCode.Created)
				.WithNoCache();
		}

		[HttpDelete]
        [RavenRoute("fs/{fileSystemName}/config")]
        public HttpResponseMessage Delete(string name)
		{
			Storage.Batch(accessor => accessor.DeleteConfig(name));

			Publisher.Publish(new ConfigurationChangeNotification { Name = name, Action = ConfigurationChangeAction.Delete });

			if (Log.IsDebugEnabled)
				Log.Debug("Config '{0}' was deleted", name);

			return GetEmptyMessage(HttpStatusCode.NoContent);
		}
	}
}
