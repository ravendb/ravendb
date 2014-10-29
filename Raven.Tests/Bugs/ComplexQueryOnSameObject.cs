using System;
using System.Collections.Generic;
using System.Linq;
using System.Net;
using Raven.Client.Document;
using Raven.Tests.Common;

using Xunit;

namespace Raven.Tests.Bugs
{
	public class ComplexQueryOnSameObject : RavenTest
	{
		[Fact]
		public void WillSucceed()
		{
			using(GetNewServer())
			{
				new WebClient().DownloadString(
					"http://localhost:8079/indexes/dynamic/AdRequests?query=-Impressions%252CClick%253A%255B%255BNULL_VALUE%255D%255D%2520AND%2520Impressions%252CClick%253A*%2520AND%2520Impressions%252CClick.ClickTime%253A%255B20110205142325841%2520TO%2520NULL%255D&start=0&pageSize=128&aggregation=None");
			}
		}

		public class AdRequest
		{
			public IEnumerable<Impression> Impressions { get; set; }
		}

		public class Impression
		{
			public Click Click { get; set; }
		}

		public class Click
		{
			public DateTime ClickTime { get; set; }
		}
	}

}