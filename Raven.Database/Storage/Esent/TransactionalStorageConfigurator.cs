//-----------------------------------------------------------------------
// <copyright file="TransactionalStorageConfigurator.cs" company="Hibernating Rhinos LTD">
//     Copyright (c) Hibernating Rhinos LTD. All rights reserved.
// </copyright>
//-----------------------------------------------------------------------

using Raven.Database.Config;
using Raven.Database.Storage.Esent;

namespace Raven.Storage.Esent
{
    public class TransactionalStorageConfigurator : StorageConfigurator
    {
        private readonly TransactionalStorage transactionalStorage;

        public TransactionalStorageConfigurator(InMemoryRavenConfiguration configuration, TransactionalStorage transactionalStorage)
            : base(configuration)
        {
            this.transactionalStorage = transactionalStorage;
        }

        protected override void ConfigureInstanceInternal(int maxVerPages)
        {
            if (transactionalStorage != null)
            {
                transactionalStorage.MaxVerPagesValueInBytes = maxVerPages * 1024 * 1024;
            }
        }

        protected override string BaseName
        {
            get
            {
                return "RVN";
            }
        }

        protected override string EventSource
        {
            get
            {
                return "Raven";
            }
        }
    }
}
