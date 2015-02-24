using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Net;
using System.Net.Http;
using System.Threading.Tasks;
using Newtonsoft.Json;
using Newtonsoft.Json.Linq;

namespace TailFeather.Client
{
	public class TailFeatherClient : IDisposable
	{
		private readonly ConcurrentDictionary<Uri, HttpClient> _cache = new ConcurrentDictionary<Uri, HttpClient>();
		private Task<TailFeatherTopology> _topologyTask;

		public TailFeatherClient(params Uri[] nodes)
		{
			_topologyTask = FindLatestTopology(nodes);
		}

		private HttpClient GetHttpClient(Uri node)
		{
			return _cache.GetOrAdd(node, uri => new HttpClient { BaseAddress = uri });
		}

		private async Task<TailFeatherTopology> FindLatestTopology(IEnumerable<Uri> nodes)
		{
			var tasks = nodes.Select(node => GetHttpClient(node).GetAsync("tailfeather/admin/flock")).ToArray();

			await Task.WhenAny(tasks);
			var topologies = new List<TailFeatherTopology>();
			foreach (var task in tasks)
			{
				var message = task.Result;
				if (message.IsSuccessStatusCode == false)
					continue;

				topologies.Add(new JsonSerializer().Deserialize<TailFeatherTopology>(
					new JsonTextReader(new StreamReader(await message.Content.ReadAsStreamAsync()))));
			}

			return topologies.OrderByDescending(x => x.CommitIndex).FirstOrDefault();
		}

		private async Task<HttpResponseMessage> ContactServer(Func<HttpClient, Task<HttpResponseMessage>> operation, int retries = 3)
		{
			if (retries < 0)
				throw new InvalidOperationException("Cluster is not reachable, or no leader was selected. Out of retries, aborting.");

			var topology = (await _topologyTask ?? new TailFeatherTopology());

			var leader = topology.AllVotingNodes.FirstOrDefault(x => x.Name == topology.CurrentLeader);
			if (leader == null)
			{
				_topologyTask = FindLatestTopology(topology.AllVotingNodes.Select(x => x.Uri));
				return await ContactServer(operation, retries - 1);
			}

			// now we have a leader, we need to try calling it...
			var httpResponseMessage = await operation(GetHttpClient(leader.Uri));
			if (httpResponseMessage.IsSuccessStatusCode == false)
			{
				// we were sent to a different server, let try that...
				if (httpResponseMessage.StatusCode == HttpStatusCode.Redirect)
				{
					var redirectUri = httpResponseMessage.Headers.Location;
					httpResponseMessage = await operation(GetHttpClient(redirectUri));
					if (httpResponseMessage.IsSuccessStatusCode)
					{
						// we successfully contacted the redirected server, this is probably the leader, let us ask it for the topology,
						// it will be there for next time we access it
						_topologyTask = FindLatestTopology(new[] { redirectUri }.Union(topology.AllVotingNodes.Select(x => x.Uri)));

						return httpResponseMessage;
					}
				}

				// we couldn't get to the server, and we didn't get redirected, we'll check in the cluster in general
				_topologyTask = FindLatestTopology(topology.AllVotingNodes.Select(x => x.Uri));
				return await ContactServer(operation, retries - 1);
			}

			// happy path, we are done
			return httpResponseMessage;
		}

		public Task Set(string key, JToken value)
		{
			return ContactServer(client => client.GetAsync(string.Format("tailfeather/key-val/set?key={0}&val={1}",
				Uri.EscapeDataString(key), Uri.EscapeDataString(value.ToString(Formatting.None)))));
		}

		public async Task<JToken> Get(string key)
		{
			var reply = await ContactServer(client => client.GetAsync(string.Format("tailfeather/key-val/read?key={0}",
				Uri.EscapeDataString(key))));
			var result = JObject.Load(new JsonTextReader(new StreamReader(await reply.Content.ReadAsStreamAsync())));

			if (result.Value<bool>("Missing"))
				return null;

			return result["Value"];
		}

		public Task Remove(string key)
		{
			return ContactServer(client => client.GetAsync(string.Format("tailfeather/key-val/del?key={0}",
				Uri.EscapeDataString(key))));
		}

		public void Dispose()
		{
			foreach (var httpClient in _cache)
			{
				httpClient.Value.Dispose();
			}
			_cache.Clear();
		}
	}
}
