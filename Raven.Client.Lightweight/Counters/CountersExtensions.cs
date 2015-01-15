using System;
using System.Collections.Generic;
using System.Linq;
using System.Net;
using System.Text;
using System.Threading.Tasks;

namespace Raven.Client.Counters
{
	public static class CountersExtensions
	{
		public static CountersClient NewCountersClient(this IDocumentStore store, string name, ICredentials credentials = null, string apiKey = null)
		{
			return new CountersClient(store.Url,name,credentials, apiKey);
		}
	}
}
