// -----------------------------------------------------------------------
//  <copyright file="DocumentWithNaN.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------
using System;
using System.Net.Http;
using System.Threading.Tasks;
using Raven.Database.Server.WebApi;
using Raven.Tests.Common;
using Xunit;

namespace Raven.Tests.Bugs
{
	public class DocumentWithNaN : RavenTest
	{
		[Fact]
		public async Task CanSaveUsingLegacyMode()
		{
			using (var store = NewRemoteDocumentStore(fiddler:true))
			{
				var httpClient = new HttpClient
				{
					BaseAddress = new Uri(store.Url)
				};

                var httpResponseMessage = await httpClient.PutAsync("docs/items/1", new MultiGetSafeStringContent("{'item': NaN}"));
				Assert.True(httpResponseMessage.IsSuccessStatusCode);
			}
		}
		 
	}
}