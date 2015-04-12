// -----------------------------------------------------------------------
//  <copyright file="StressTestIssue.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------
using System;
using System.Collections.Generic;
using Raven.Client;
using Raven.Tests.Helpers;
using Xunit;

namespace Raven.SlowTests.MailingList
{
	public class StreamIssueTest : RavenTestBase
	{
		[Fact]
		public void CanStreamFromLocal()
		{
			using (var store = NewDocumentStore())
			{
				StoreSamples(store);
				ReadSamples(store);
			}
		}

		[Fact]
		public void CanStreamFromRemote()
		{
			using (var store = NewRemoteDocumentStore())
			{
				StoreSamples(store);
				// Reading the samples will eventually cause an exception: 
				// "System.IO.IOException - Unable to read data from the transport connection: The connection was closed."
				ReadSamples(store);
			}
		}

		private void StoreSamples(IDocumentStore store)
		{
			using (var session = store.OpenSession())
			{
				for (var i = 0; i < 1500; i++)
				{
					session.Store(new Sample {Id = Guid.NewGuid(), Name = new string('a', 10000)});
				}

				session.SaveChanges();
			}
		}

		private void ReadSamples(IDocumentStore store)
		{
			using (var session = store.OpenSession())
			{
				using (var stream = session.Advanced.Stream<Sample>("Samples/"))
				{
					var samples = new List<Sample>();
					while (stream.MoveNext())
					{
						samples.Add(stream.Current.Document);
					}

					Assert.Equal(1500, samples.Count);
				}
			}
		}
		public class Sample
		{
			public Guid Id { get; set; }
			public string Name { get; set; }
		}

	}


}