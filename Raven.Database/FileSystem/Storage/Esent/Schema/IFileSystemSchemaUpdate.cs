//-----------------------------------------------------------------------
// <copyright file="ISchemaUpdate.cs" company="Hibernating Rhinos LTD">
//     Copyright (c) Hibernating Rhinos LTD. All rights reserved.
// </copyright>
//-----------------------------------------------------------------------
using System;
using System.ComponentModel.Composition;
using Microsoft.Isam.Esent.Interop;
using Raven.Database.Config;

namespace Raven.Database.FileSystem.Storage.Esent.Schema
{
    [InheritedExport]
    public interface IFileSystemSchemaUpdate
    {
        string FromSchemaVersion { get;  }
        void Init(InMemoryRavenConfiguration configuration);
        void Update(Session session, JET_DBID dbid, Action<string> output);
    }
}
