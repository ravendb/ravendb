// -----------------------------------------------------------------------
//  <copyright file="RavenDB-1285.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------
using System;
using System.Threading.Tasks;
using Raven.Client.Linq;
using Raven.Tests.Common;

using Xunit;

namespace Raven.Tests.Issues
{
	public class RavenDB_1285 : RavenTest
	{
		public class LanguageEntries 
		{
			public string Language { get; set; }
		}

		[Fact]
		public void ReturnedExceptionShouldContainInfoThatTheyCantStreamOnDynamicIndex_Remote()
		{
			using (var store = NewRemoteDocumentStore())
			{
				using (var session = store.OpenSession())
				{
					var query = session.Query<LanguageEntries>().Where(x => x.Language == "en_US");

					var ex = Assert.Throws<InvalidOperationException>(() =>
					{
						session.Advanced.Stream(query);
					});

					Assert.Contains("StreamQuery does not support querying dynamic indexes", ex.Message);
				}
			}
		}

		[Fact]
		public async Task ReturnedExceptionShouldContainInfoThatTheyCantStreamOnDynamicIndex_Remote_Async()
		{
			using (var store = NewRemoteDocumentStore())
			{
				using (var session = store.OpenAsyncSession())
				{
					var query = session.Query<LanguageEntries>().Where(x => x.Language == "en_US");

					InvalidOperationException ex = null;

					try
					{
						await session.Advanced.StreamAsync(query);
					}
					catch (InvalidOperationException e)
					{
						ex = e;
					}

					Assert.Contains("StreamQuery does not support querying dynamic indexes", ex.Message);
				}
			}
		}

		[Fact]
		public void ReturnedExceptionShouldContainInfoThatTheyCantStreamOnDynamicIndex_Embedded()
		{
			using (var store = NewDocumentStore())
			{
				using (var session = store.OpenSession())
				{
					var query = session.Query<LanguageEntries>().Where(x => x.Language == "en_US");

					var ex = Assert.Throws<InvalidOperationException>(() =>
					{
						var enumerator = session.Advanced.Stream(query);

						while (enumerator.MoveNext())
						{
							
						}
					});

					Assert.Contains("StreamQuery does not support querying dynamic indexes", ex.Message);
				}
			}
		}

		[Fact]
		public async Task ReturnedExceptionShouldContainInfoThatTheyCantStreamOnDynamicIndex_Embedded_Async()
		{
			using (var store = NewDocumentStore())
			{
				using (var session = store.OpenAsyncSession())
				{
					var query = session.Query<LanguageEntries>().Where(x => x.Language == "en_US");

					InvalidOperationException ex = null;

					try
					{
						var enumerator = await session.Advanced.StreamAsync(query);

						while (await enumerator.MoveNextAsync())
						{

						}
					}
					catch (InvalidOperationException e)
					{
						ex = e;
					}

					Assert.Contains("StreamQuery does not support querying dynamic indexes", ex.Message);
				}
			}
		}
	}
}