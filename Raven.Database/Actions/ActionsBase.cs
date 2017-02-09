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
            "Etag",
            Constants.RavenLastModified
        };

        protected DocumentDatabase Database { get; private set; }

        protected ILog Log { get; private set; }

        [CLSCompliant(false)]
        protected ITransactionalStorage TransactionalStorage
        {
            get { return Database.TransactionalStorage; }
        }

        protected WorkContext WorkContext
        {
            get { return Database.WorkContext; }
        }

        protected IndexDefinitionStorage IndexDefinitionStorage
        {
            get { return Database.IndexDefinitionStorage; }
        }

        protected IUuidGenerator UuidGenerator { get; private set; }

        protected ActionsBase(DocumentDatabase database, IUuidGenerator uuidGenerator, ILog log)
        {
            Database = database;
            UuidGenerator = uuidGenerator;
            Log = log;
        }
    }
}
