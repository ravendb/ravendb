//using System;
//using System.Collections.Generic;
//using System.Linq;
//using System.Net;
//using System.Text;
//using System.Threading.Tasks;
//using Raven.Client;
//using Raven.Client.Counters;
//using Raven.Server;
//using Raven.Tests.Helpers;
//
//namespace Raven.Tests.Counters
//{
//	public class BaseCountersTest : RavenTestBase
//	{
//		protected IDocumentStore ravenStore;
//		private const string DefaultCountesClientName = "FooBarCounters";
//
//		public BaseCountersTest()
//		{
//			ravenStore = NewRemoteDocumentStore();
//		}
//
//		public CountersClient NewCountersClient(ICredentials credentials = null, string apiKey = null)
//		{
//			return ravenStore.NewCountersClient(DefaultCountesClientName,credentials,apiKey);
//		}
//
//		public override void Dispose()
//		{
//			base.Dispose();
//
//			if (ravenStore != null) ravenStore.Dispose();
//		}
//	}
//}
