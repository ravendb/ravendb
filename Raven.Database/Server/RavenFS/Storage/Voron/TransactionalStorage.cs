// -----------------------------------------------------------------------
//  <copyright file="TransactionalStorage.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------
using System;

using Raven.Database.Server.RavenFS.Storage.Esent;

namespace Raven.Database.Server.RavenFS.Storage.Voron
{
    public class TransactionalStorage : ITransactionalStorage
    {
        public void Dispose()
        {
            throw new NotImplementedException();
        }

        public string Database { get; private set; }

        public Guid Id { get; private set; }

        public bool Initialize()
        {
            throw new NotImplementedException();
        }

        public void Batch(Action<IStorageActionsAccessor> action)
        {
            throw new NotImplementedException();
        }
    }
}