// -----------------------------------------------------------------------
//  <copyright file="ResourceBackupState.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------
using System.Collections.Generic;

namespace Raven.Abstractions.Data
{
    public class InternalStorageBreakdownState : OperationStateBase
    {
        public IList<string> ReportResults { get; set; }
    }
}