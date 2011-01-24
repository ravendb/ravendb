//-----------------------------------------------------------------------
// <copyright file="LinearQueryRunner.cs" company="Hibernating Rhinos LTD">
//     Copyright (c) Hibernating Rhinos LTD. All rights reserved.
// </copyright>
//-----------------------------------------------------------------------
using System;
using System.Linq;
using Newtonsoft.Json.Linq;
using Raven.Database.Data;
using Raven.Database.Queries.LinearQueries;

namespace Raven.Database.Queries
{
    public class LinearQueryRunner : IDisposable
    {
        private readonly DocumentDatabase database;
        private QueryRunnerManager queryRunnerManager;
        private AppDomain queriesAppDomain;

        public LinearQueryRunner(DocumentDatabase database)
        {
            this.database = database;
        }

        public QueryResults ExecuteQueryUsingLinearSearch(LinearQuery query)
        {
            if (queriesAppDomain == null)
            {
                lock (this)
                {
                    if (queriesAppDomain == null)
                        InitailizeQueriesAppDomain();
                }
            }

            query.PageSize = Math.Min(query.PageSize, database.Configuration.MaxPageSize);

            RemoteQueryResults result;
            using (var remoteSingleQueryRunner = queryRunnerManager.CreateSingleQueryRunner(
                database.TransactionalStorage.TypeForRunningQueriesInRemoteAppDomain,
                database.TransactionalStorage.StateForRunningQueriesInRemoteAppDomain))
            {
                result = remoteSingleQueryRunner.Query(query);
            }


            if (result.QueryCacheSize > 1024)
            {
                lock (this)
                {
                    if (queryRunnerManager.QueryCacheSize > 1024)
                        UnloadQueriesAppDomain();
                }
            }
            return new QueryResults
            {
                LastScannedResult = result.LastScannedResult,
                TotalResults = result.TotalResults,
                Errors = result.Errors,
                Results = result.Results.Select(JObject.Parse).ToArray()
            };

        }

        private void UnloadQueriesAppDomain()
        {
            queryRunnerManager = null;
            if (queriesAppDomain != null)
                AppDomain.Unload(queriesAppDomain);
        }

        private void InitailizeQueriesAppDomain()
        {
            queriesAppDomain = AppDomain.CreateDomain("Queries", null, AppDomain.CurrentDomain.SetupInformation);
            queryRunnerManager = (QueryRunnerManager)queriesAppDomain.CreateInstanceAndUnwrap(typeof(QueryRunnerManager).Assembly.FullName, typeof(QueryRunnerManager).FullName);
        }

        /// <summary>
        /// Performs application-defined tasks associated with freeing, releasing, or resetting unmanaged resources.
        /// </summary>
        /// <filterpriority>2</filterpriority>
        public void Dispose()
        {
            UnloadQueriesAppDomain();
        }
    }
}