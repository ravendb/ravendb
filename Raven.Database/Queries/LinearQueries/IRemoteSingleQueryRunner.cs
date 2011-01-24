//-----------------------------------------------------------------------
// <copyright file="IRemoteSingleQueryRunner.cs" company="Hibernating Rhinos LTD">
//     Copyright (c) Hibernating Rhinos LTD. All rights reserved.
// </copyright>
//-----------------------------------------------------------------------
using System;
using Raven.Database.Data;

namespace Raven.Database.Queries.LinearQueries
{
    public interface IRemoteSingleQueryRunner : IDisposable
    {
        RemoteQueryResults Query(LinearQuery query);
    }
}