// -----------------------------------------------------------------------
//  <copyright file="RavenDB_4708_Conventions.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------

using System;
using System.Linq;
using System.Reflection;
using System.Threading.Tasks;
using FastTests;
using Newtonsoft.Json.Linq;
using Raven.Client.Documents.Linq;
using Raven.Client.Documents.Queries.Facets;
using Raven.Client.Documents.Session;
using Raven.Client.Extensions;
using Raven.Tests.Core.Utils.Entities;
using Xunit;

namespace SlowTests.Issues
{
    public class RavenDB_4708_Conventions : NoDisposalNeeded
    {
        [Fact]
        public void Sync_document_query_can_not_have_async_methods()
        {
            var documentQueryType = typeof(DocumentQuery<JObject>);

            var methods = documentQueryType.GetMethods(BindingFlags.Instance | BindingFlags.Public);

            var asyncMethods = methods.Where(m => m.Name.EndsWith("Async")).ToList();

            Assert.True(asyncMethods.Count == 0, "Async methods detected in sync interface: " + string.Join(", ", asyncMethods.Select(x => x.ReturnType + " " + x.Name)));
        }

        [Fact]
        public void Async_document_query_can_not_have_sync_materialization_methods()
        {
            var asyncDocumentQueryType = typeof(AsyncDocumentQuery<Employee>);
            var methods = asyncDocumentQueryType.GetMethods(BindingFlags.Instance | BindingFlags.Public);

            // the only suspected methods are the one which contains Employee in ToString representation
            // filter them out
            var suspectedMethods = methods.Where(x => x.ReturnType.ToString().Contains(typeof(Employee).FullName)).ToList();

            // the ones which return IAsyncDocumentQuery<Employee> are ok
            suspectedMethods = suspectedMethods.Where(x =>
                x.ReturnType != typeof(IRavenQueryable<Employee>) &&
                x.ReturnType != typeof(IAsyncDocumentQuery<Employee>) && 
                x.ReturnType != typeof(IAsyncGraphQuery<Employee>) &&
                x.ReturnType != typeof(IAsyncGroupByDocumentQuery<Employee>) &&
                x.ReturnType != typeof(IAsyncAggregationDocumentQuery<Employee>))
                .ToList();

            // filter out Lazy methods
            suspectedMethods = suspectedMethods.Where(x => !x.ReturnType.GetTypeInfo().IsGenericType || typeof(Lazy<>) != x.ReturnType.GetGenericTypeDefinition()).ToList();

            // filter out methods which returns Task
            suspectedMethods = suspectedMethods.Where(x => !x.ReturnType.GetTypeInfo().IsGenericType || typeof(Task<>) != x.ReturnType.GetGenericTypeDefinition()).ToList();

            // filter out where / group by methods as it only servs as reminder to not use in memory filtering
            suspectedMethods = suspectedMethods.Where(x => x.Name != "Where" && x.Name != "GroupBy").ToList();

            Assert.True(suspectedMethods.Count == 0, "Detected sync methods in async interface: " + string.Join(", ", suspectedMethods.Select(x => x.ReturnType + " " + x.Name)));
        }

        [Fact]
        public void Sync_and_async_interfaces_should_contain_same_methods()
        {
            var asyncDocumentQueryType = typeof(AsyncDocumentQuery<Employee>);
            var syncDocumentQueryType = typeof(DocumentQuery<Employee>);

            Func<Type, MethodInfo[]> methodExtractor = type => type.GetMethods(BindingFlags.Public | BindingFlags.Instance);

            var asyncMethods = methodExtractor(asyncDocumentQueryType)
                .Select(x => x.Name.EndsWith("Async") ? x.Name.Substring(0, x.Name.Length - "Async".Length) : x.Name)
                .ToHashSet();
            var syncMethods = methodExtractor(syncDocumentQueryType)
                .Select(x => x.Name)
                .ToHashSet();

            // now compute symmetric difference 
            var onlyInSyncVersion = syncMethods.Except(asyncMethods).ToList();
            var onlyInAsyncVersion = asyncMethods.Except(syncMethods).ToList();

            //since we have GetEnumerator in sync and ToListAsync in async ignore them
            onlyInAsyncVersion.Remove("ToList");
            onlyInSyncVersion.Remove("GetEnumerator");

            // we have small differences which we ignore:
            onlyInAsyncVersion.Remove("get_AsyncIndexQueried");
            onlyInSyncVersion.Remove("get_IndexQueried");
            onlyInAsyncVersion.Remove("QueryResult");
            onlyInSyncVersion.Remove("get_QueryResult");

            Assert.True(onlyInSyncVersion.Count == 0, "We have methods in sync interface which are not available in async: " + string.Join(", ", onlyInSyncVersion));
            Assert.True(onlyInAsyncVersion.Count == 0, "We have methods in async interface which are not available in sync: " + string.Join(", ", onlyInAsyncVersion));
        }
    }
}
