// -----------------------------------------------------------------------
//  <copyright file="RavenDB_752.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------
using System;
using System.Collections.Generic;
using System.Net;
using Raven.Abstractions.Extensions;
using Raven.Abstractions.Util;
using Raven.Client.Connection;
using Raven.Client.Document;
using Xunit;

namespace Raven.Tests.Issues
{
	public class RavenDB_752
	{
		[Fact]
		public void ReplicationInformerShouldThrowAfterSecondTimeout()
		{
			var replicationInformer = new ReplicationInformer(new DocumentConvention
			{
				FailoverBehavior = FailoverBehavior.AllowReadsFromSecondariesAndWritesToSecondaries
			})
			{
				ReplicationDestinations =
					{
						new ReplicationDestinationData
						{
							Url = "http://localhost:2"
						},
						new ReplicationDestinationData
						{
							Url = "http://localhost:3"
						},
						new ReplicationDestinationData
						{
							Url = "http://localhost:4"
						},
					}
			};

			var urlsTried = new List<string>();

			var webException = Assert.Throws<WebException>(() =>
			{
				replicationInformer.ExecuteWithReplication("GET", "http://localhost:1", 1, 1, url =>
				{
					urlsTried.Add(url);
					throw new WebException("Timeout", WebExceptionStatus.Timeout);

					return 1;
				});
			});

			Assert.Equal(2, urlsTried.Count);
			Assert.Equal("http://localhost:1", urlsTried[0]);
			Assert.Equal("http://localhost:2", urlsTried[1]);

			Assert.Equal(WebExceptionStatus.Timeout, webException.Status);
		}

		[Fact]
		public void ReplicationInformerShouldThrowAfterSecondTimeoutIfReadStripingEnabled()
		{
			var replicationInformer = new ReplicationInformer(new DocumentConvention
			{
				FailoverBehavior = FailoverBehavior.ReadFromAllServers
			})
			{
				ReplicationDestinations =
					{
						new ReplicationDestinationData
						{
							Url = "http://localhost:2"
						},
						new ReplicationDestinationData
						{
							Url = "http://localhost:3"
						},
						new ReplicationDestinationData
						{
							Url = "http://localhost:4"
						},
					}
			};

			var urlsTried = new List<string>();

			var webException = Assert.Throws<WebException>(() =>
			{
				replicationInformer.ExecuteWithReplication("GET", "http://localhost:1", 1, 1, url =>
				{
					urlsTried.Add(url);
					throw new WebException("Timeout", WebExceptionStatus.Timeout);

					return 1;
				});
			});

			Assert.Equal(2, urlsTried.Count);
			Assert.Equal("http://localhost:3", urlsTried[0]); // striped
			Assert.Equal("http://localhost:1", urlsTried[1]); // master

			Assert.Equal(WebExceptionStatus.Timeout, webException.Status);
		}

		[Fact]
		public void ReplicationInformerShouldThrowAfterSecondTimeout_Async()
		{
			var replicationInformer = new ReplicationInformer(new DocumentConvention
			{
				FailoverBehavior = FailoverBehavior.AllowReadsFromSecondariesAndWritesToSecondaries
			})
			{
				ReplicationDestinations =
					{
						new ReplicationDestinationData
						{
							Url = "http://localhost:2"
						},
						new ReplicationDestinationData
						{
							Url = "http://localhost:3"
						},
						new ReplicationDestinationData
						{
							Url = "http://localhost:4"
						},
					}
			};

			var urlsTried = new List<string>();

			var aggregateException = Assert.Throws<AggregateException>(() =>
				replicationInformer.ExecuteWithReplicationAsync<int>("GET", "http://localhost:1", 1, 1, url =>
				{
					urlsTried.Add(url);

					return new CompletedTask<int>(new WebException("Timeout", WebExceptionStatus.Timeout));
				}).Wait()
			);

			var webException = aggregateException.ExtractSingleInnerException() as WebException;
			Assert.NotNull(webException);
			Assert.Equal(WebExceptionStatus.Timeout, webException.Status);

			Assert.Equal(2, urlsTried.Count);
			Assert.Equal("http://localhost:1", urlsTried[0]);
			Assert.Equal("http://localhost:2", urlsTried[1]);
		}

		[Fact]
		public void ReplicationInformerShouldThrowAfterSecondTimeoutIfReadStripingEnabled_Async()
		{
			var replicationInformer = new ReplicationInformer(new DocumentConvention
			{
				FailoverBehavior = FailoverBehavior.ReadFromAllServers
			})
			{
				ReplicationDestinations =
					{
						new ReplicationDestinationData
						{
							Url = "http://localhost:2"
						},
						new ReplicationDestinationData
						{
							Url = "http://localhost:3"
						},
						new ReplicationDestinationData
						{
							Url = "http://localhost:4"
						},
					}
			};

			var urlsTried = new List<string>();

			var aggregateException = Assert.Throws<AggregateException>(() =>
				replicationInformer.ExecuteWithReplicationAsync<int>("GET", "http://localhost:1", 1, 1, url =>
				{
					urlsTried.Add(url);

					return new CompletedTask<int>(new WebException("Timeout", WebExceptionStatus.Timeout));
				}).Wait()
			);

			var webException = aggregateException.ExtractSingleInnerException() as WebException;
			Assert.NotNull(webException);
			Assert.Equal(WebExceptionStatus.Timeout, webException.Status);

			Assert.Equal(2, urlsTried.Count);
			Assert.Equal("http://localhost:3", urlsTried[0]); // striped
			Assert.Equal("http://localhost:1", urlsTried[1]); // master
		}
	}
}