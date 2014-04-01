// -----------------------------------------------------------------------
//  <copyright file="ReplicationBehavior.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------
using System;
using System.Collections.Generic;
using System.Linq;
using System.Net;
using Raven.Client.Connection;
using Raven.Client.Document;
using Raven.Tests.Common;

using Xunit;

namespace Raven.Tests.Issues
{
	using Raven.Abstractions.Connection;

	public class ReplicationBehavior : NoDisposalNeeded
	{
		[Fact]
		public void BackoffStrategy()
		{
			var replicationInformer = new ReplicationInformer(new DocumentConvention())
			{
				ReplicationDestinations =
					{
						new OperationMetadata("http://localhost:2")
					}
			};

			var urlsTried = new List<Tuple<int, string>>();
			for (int i = 0; i < 5000; i++)
			{
				var req = i + 1;
				replicationInformer.ExecuteWithReplication("GET", "http://localhost:1", new OperationCredentials(null, CredentialCache.DefaultNetworkCredentials), req, 1, url =>
				{
					urlsTried.Add(Tuple.Create(req, url.Url));
					if (url.Url.EndsWith("1"))
						throw new WebException("bad", WebExceptionStatus.ConnectFailure);

					return 1;
				});
			}
			var expectedUrls = GetExpectedUrlForFailure().Take(urlsTried.Count).ToList();

			Assert.Equal(expectedUrls, urlsTried);
		}

		[Fact]
		public void ReadStriping()
		{
			var replicationInformer = new ReplicationInformer(new DocumentConvention
			{
				FailoverBehavior = FailoverBehavior.ReadFromAllServers
			})
			{
				ReplicationDestinations =
					{
						new OperationMetadata("http://localhost:2"),
						new OperationMetadata("http://localhost:3"),
						new OperationMetadata("http://localhost:4"),
					}
			};

			var urlsTried = new List<Tuple<int, string>>();
			for (int i = 0; i < 10; i++)
			{
				var req = i + 1;
				replicationInformer.ExecuteWithReplication("GET", "http://localhost:1", new OperationCredentials(null, CredentialCache.DefaultNetworkCredentials), req, req, url =>
				{
					urlsTried.Add(Tuple.Create(req, url.Url));
					return 1;
				});
			}
			var expectedUrls = GetExpectedUrlForReadStriping().Take(urlsTried.Count).ToList();

			Assert.Equal(expectedUrls, urlsTried);
		}

		private IEnumerable<Tuple<int, string>> GetExpectedUrlForReadStriping()
		{
			int reqCount = 0;
			var urls = new[]
			{
				"http://localhost:2",
				"http://localhost:3",
				"http://localhost:4",

			};
			while (true)
			{
				reqCount++;
				var pos = reqCount % (urls.Length + 1);
				if (pos >= urls.Length)
					yield return Tuple.Create(reqCount, "http://localhost:1");
				else
					yield return Tuple.Create(reqCount, urls[pos]);
			}
		}

		private IEnumerable<Tuple<int, string>> GetExpectedUrlForFailure()
		{
			int reqCount = 1;
			var failCount = 0;
			// first time, we check it twice
			yield return Tuple.Create(reqCount, "http://localhost:1");
			yield return Tuple.Create(reqCount, "http://localhost:1");
			failCount++;
			yield return Tuple.Create(reqCount, "http://localhost:2");

			while (failCount < 10)
			{
				reqCount++;
				if (reqCount % 2 == 0)
				{
					yield return Tuple.Create(reqCount, "http://localhost:1");
					failCount++;
					yield return Tuple.Create(reqCount, "http://localhost:2");
				}
				else
				{
					yield return Tuple.Create(reqCount, "http://localhost:2");
				}
			}

			while (failCount < 100)
			{
				reqCount++;
				if (reqCount % 10 == 0)
				{
					yield return Tuple.Create(reqCount, "http://localhost:1");
					failCount++;
					yield return Tuple.Create(reqCount, "http://localhost:2");
				}
				else
				{
					yield return Tuple.Create(reqCount, "http://localhost:2");
				}
			}
			while (failCount < 1000)
			{
				reqCount++;
				if (reqCount % 100 == 0)
				{
					yield return Tuple.Create(reqCount, "http://localhost:1");
					failCount++;
					yield return Tuple.Create(reqCount, "http://localhost:2");
				}
				else
				{
					yield return Tuple.Create(reqCount, "http://localhost:2");
				}
			}
			while (failCount < 10000)
			{
				reqCount++;
				if (reqCount % 1000 == 0)
				{
					yield return Tuple.Create(reqCount, "http://localhost:1");
					failCount++;
					yield return Tuple.Create(reqCount, "http://localhost:2");
				}
				else
				{
					yield return Tuple.Create(reqCount, "http://localhost:2");
				}
			}
		}
	}
}