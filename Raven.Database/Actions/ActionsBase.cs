// -----------------------------------------------------------------------
//  <copyright file="ActionsBase.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------
using System;
using System.Collections.Generic;

using Raven.Abstractions.Data;
using Raven.Abstractions.Logging;
using Raven.Database.Data;
using Raven.Database.Impl;
using Raven.Database.Indexing;
using Raven.Database.Storage;
using Raven.Database.Util;

namespace Raven.Database.Actions
{
    public abstract class ActionsBase
    {
        protected static readonly HashSet<string> HeadersToIgnoreServer = new HashSet<string>(StringComparer.OrdinalIgnoreCase)
		{
			Constants.RavenLastModified
		};

        protected SizeLimitedConcurrentDictionary<string, TouchedDocumentInfo> RecentTouches { get; private set; }

        protected DocumentDatabase Database { get; private set; }

        protected ILog Log { get; private set; }

        protected ITransactionalStorage TransactionalStorage { get; private set; }

        protected WorkContext WorkContext { get; private set; }

        protected IndexDefinitionStorage IndexDefinitionStorage { get; private set; }

        protected IUuidGenerator UuidGenerator { get; private set; }

        protected ActionsBase(DocumentDatabase database, SizeLimitedConcurrentDictionary<string, TouchedDocumentInfo> recentTouches, IUuidGenerator uuidGenerator, ILog log)
        {
            Database = database;
            RecentTouches = recentTouches;
            UuidGenerator = uuidGenerator;
            Log = log;
            TransactionalStorage = database.TransactionalStorage;
            WorkContext = database.WorkContext;
            IndexDefinitionStorage = database.IndexDefinitionStorage;
        }
    }
}