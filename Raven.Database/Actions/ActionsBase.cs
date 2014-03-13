// -----------------------------------------------------------------------
//  <copyright file="ActionsBase.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------
using Raven.Database.Impl.DTC;
using Raven.Database.Indexing;
using Raven.Database.Storage;

namespace Raven.Database.Actions
{
    public abstract class ActionsBase
    {
        protected DocumentDatabase Database { get; private set; }

        protected ITransactionalStorage TransactionalStorage { get; private set; }

        protected WorkContext WorkContext { get; private set; }

        protected InFlightTransactionalState InFlightTransactionalState { get; private set; }

        protected IndexDefinitionStorage IndexDefinitionStorage { get; private set; }

        protected ActionsBase(DocumentDatabase database)
        {
            Database = database;
            TransactionalStorage = database.TransactionalStorage;
            WorkContext = database.WorkContext;
            InFlightTransactionalState = database.InFlightTransactionalState;
            IndexDefinitionStorage = database.IndexDefinitionStorage;
        }
    }
}