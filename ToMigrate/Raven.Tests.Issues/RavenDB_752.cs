// -----------------------------------------------------------------------
//  <copyright file="RavenDB_752.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------
using System;
using System.Collections.Generic;
using System.Net;
using Raven.Abstractions.Connection;
using Raven.Abstractions.Extensions;
using Raven.Abstractions.Replication;
using Raven.Abstractions.Util;
using Raven.Client.Connection;
using Raven.Client.Document;
using Raven.Client.Metrics;
using Raven.Tests.Common;

using Xunit;

namespace Raven.Tests.Issues
{
    public class RavenDB_752 : NoDisposalNeeded
    {
        private const int MaxNumber = 2048;
        [Fact]
        public void ReplicationInformerShouldThrowAfterSecondTimeout()
        {
            using (var replicationInformer = new ReplicationInformer(new DocumentConvention
            {
                FailoverBehavior = FailoverBehavior.AllowReadsFromSecondariesAndWritesToSecondaries
            }, new HttpJsonRequestFactory(MaxNumber), s => new RequestTimeMetric())
            {
                ReplicationDestinations =
                    {
                        new OperationMetadata("http://localhost:2"),
                        new OperationMetadata("http://localhost:3"),
                        new OperationMetadata("http://localhost:4")
                    }
            })
            {
                var urlsTried = new List<string>();

                var webException = (WebException)Assert.Throws<AggregateException>(() =>
                    replicationInformer.ExecuteWithReplicationAsync<int>(HttpMethods.Get, "http://localhost:1", new OperationCredentials(null, CredentialCache.DefaultNetworkCredentials), 1, 1, (url, rtm) =>
                {
                    urlsTried.Add(url.Url);
                    throw new WebException("Timeout", WebExceptionStatus.Timeout);
                }).Wait()).ExtractSingleInnerException();

                Assert.Equal(2, urlsTried.Count);
                Assert.Equal("http://localhost:1", urlsTried[0]);
                Assert.Equal("http://localhost:2", urlsTried[1]);

                Assert.Equal(WebExceptionStatus.Timeout, webException.Status);
            }
        }

        [Fact]
        public void ReplicationInformerShouldThrowAfterSecondTimeoutIfReadStripingEnabled()
        {
            using (var replicationInformer = new ReplicationInformer(new DocumentConvention
            {
                FailoverBehavior = FailoverBehavior.ReadFromAllServers
            }, new HttpJsonRequestFactory(MaxNumber), s => new RequestTimeMetric())
            {
                ReplicationDestinations =
                    {
                        new OperationMetadata("http://localhost:2"),
                        new OperationMetadata("http://localhost:3"),
                        new OperationMetadata("http://localhost:4")
                    }
            })
            {
                var urlsTried = new List<string>();

                var webException = (WebException)Assert.Throws<AggregateException>(() =>
                   replicationInformer.ExecuteWithReplicationAsync<int>(HttpMethods.Get, "http://localhost:1", new OperationCredentials(null, CredentialCache.DefaultNetworkCredentials), 1, 1, (url, rtm) =>
               {
                   urlsTried.Add(url.Url);
                   throw new WebException("Timeout", WebExceptionStatus.Timeout);
               }).Wait()).ExtractSingleInnerException();

                Assert.Equal(2, urlsTried.Count);
                Assert.Equal("http://localhost:3", urlsTried[0]); // striped
                Assert.Equal("http://localhost:1", urlsTried[1]); // master

                Assert.Equal(WebExceptionStatus.Timeout, webException.Status);
            }
        }

        [Fact]
        public void ReplicationInformerShouldThrowAfterSecondTimeout_Async()
        {
            using (var replicationInformer = new ReplicationInformer(new DocumentConvention
            {
                FailoverBehavior = FailoverBehavior.AllowReadsFromSecondariesAndWritesToSecondaries
            }, new HttpJsonRequestFactory(MaxNumber), s => new RequestTimeMetric())
            {
                ReplicationDestinations =
                    {
                        new OperationMetadata("http://localhost:2"),
                        new OperationMetadata("http://localhost:3"),
                        new OperationMetadata("http://localhost:4")
                    }
            })
            {
                var urlsTried = new List<string>();

                var aggregateException = Assert.Throws<AggregateException>(() =>
                    replicationInformer.ExecuteWithReplicationAsync<int>(HttpMethods.Get, "http://localhost:1", new OperationCredentials(null, CredentialCache.DefaultNetworkCredentials), 1, 1, (url, rtm) =>
                    {
                        urlsTried.Add(url.Url);

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
        }

        [Fact]
        public void ReplicationInformerShouldThrowAfterSecondTimeoutIfReadStripingEnabled_Async()
        {
            using (var replicationInformer = new ReplicationInformer(new DocumentConvention
            {
                FailoverBehavior = FailoverBehavior.ReadFromAllServers
            }, new HttpJsonRequestFactory(MaxNumber), s => new RequestTimeMetric())
            {
                ReplicationDestinations =
                    {
                        new OperationMetadata("http://localhost:2"),
                        new OperationMetadata("http://localhost:3"),
                        new OperationMetadata("http://localhost:4")
                    }
            })
            {
                var urlsTried = new List<string>();

                var aggregateException = Assert.Throws<AggregateException>(() =>
                    replicationInformer.ExecuteWithReplicationAsync<int>(HttpMethods.Get, "http://localhost:1", new OperationCredentials(null, CredentialCache.DefaultNetworkCredentials), 1, 1, (url, rtm) =>
                    {
                        urlsTried.Add(url.Url);

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
}
