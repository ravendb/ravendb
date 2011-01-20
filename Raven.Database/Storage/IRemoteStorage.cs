//-----------------------------------------------------------------------
// <copyright file="IRemoteStorage.cs" company="Hibernating Rhinos LTD">
//     Copyright (c) Hibernating Rhinos LTD. All rights reserved.
// </copyright>
//-----------------------------------------------------------------------
using System;

namespace Raven.Database.Storage
{
    public interface IRemoteStorage : IDisposable
    {
        void Batch(Action<IStorageActionsAccessor> action);
    }
}