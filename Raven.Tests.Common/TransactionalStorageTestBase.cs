// -----------------------------------------------------------------------
//  <copyright file="TransactionalStorageTestBase.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------

using System;

using Raven.Abstractions;

namespace Raven.Tests.Common
{
    public abstract class TransactionalStorageTestBase : RavenTest
    {
        protected DateTime UtcNow { get; private set; }

        protected TransactionalStorageTestBase()
        {
            UtcNow = DateTime.UtcNow;
            SystemTime.UtcDateTime = () => UtcNow;
        }

        public override void Dispose()
        {
            SystemTime.UtcDateTime = null;

			base.Dispose();
        }
    }
}