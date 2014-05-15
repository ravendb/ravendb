// -----------------------------------------------------------------------
//  <copyright file="RavenDB_2181.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Text;

using Raven.Database.Client;
using Raven.Tests.Common;

using Xunit;

namespace Raven.Tests.Issues
{
	public class RavenDB_2181 : NoDisposalNeeded
	{
		[Fact(Skip = "Requires Windows Azure Development Storage")]
		public void PutBlob()
		{
			var containerName = "testContainer";
			var blobKey = "testKey";

			using (var client = new RavenAzureClient("devstoreaccount1", "Eby8vdM02xNOcqFlqUwJPLlmEtlCDXJ1OUzFT50uSRZ6IFsuFq2UVErCz4I6tq/K1SZFPTOtr/KBHBeksoGMGw==", true))
			{
				client.PutContainer(containerName);
				client.PutBlob(containerName, blobKey, new MemoryStream(Encoding.UTF8.GetBytes("123")), new Dictionary<string, string>
				                                                                                        {
					                                                                                        { "property1", "value1" }, 
																											{ "property2", "value2" }
				                                                                                        });
				var blob = client.GetBlob(containerName, blobKey);
				Assert.NotNull(blob);

				using (var reader = new StreamReader(blob.Data))
					Assert.Equal("123", reader.ReadToEnd());

				var property1 = blob.Metadata.Keys.Single(x => x.Contains("property1"));
				var property2 = blob.Metadata.Keys.Single(x => x.Contains("property2"));

				Assert.Equal("value1", blob.Metadata[property1]);
				Assert.Equal("value2", blob.Metadata[property2]);
			}
		}
	}
}