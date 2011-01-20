//-----------------------------------------------------------------------
// <copyright file="RemoteEsentStorageState.cs" company="Hibernating Rhinos LTD">
//     Copyright (c) Hibernating Rhinos LTD. All rights reserved.
// </copyright>
//-----------------------------------------------------------------------
using System;
using Microsoft.Isam.Esent.Interop;

namespace Raven.Storage.Esent
{
    [Serializable]
    public class RemoteEsentStorageState
    {
        public JET_INSTANCE Instance { get; set; }
        public string Database { get; set; }
    }
}
