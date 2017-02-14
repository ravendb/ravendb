// -----------------------------------------------------------------------
//  <copyright file="FreeSpaceHandlingDisabler.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------
using System;

namespace Voron.Impl.FreeSpace
{
    public class FreeSpaceHandlingDisabler : IDisposable
    {
        public int DisableCount;

        public void Dispose()
        {
            DisableCount--;
        }
    }
}