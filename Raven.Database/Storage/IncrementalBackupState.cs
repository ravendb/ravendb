// -----------------------------------------------------------------------
//  <copyright file="IncrementalBackupState.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------
using System;

namespace Raven.Database.Storage
{
    public class IncrementalBackupState
    {
        public Guid ResourceId { get; set; }
        public string ResourceName { get; set; }
    }
}
