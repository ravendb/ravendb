// -----------------------------------------------------------------------
//  <copyright file="ThreadInfo.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------
using System.Collections.Generic;

namespace Raven.Debug
{
    internal class ThreadInfo
    {
        public uint OSThreadId;

        public bool IsNative;

        public List<string> StackTrace = new List<string>();
    }
}
