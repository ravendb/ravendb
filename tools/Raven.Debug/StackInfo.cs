// -----------------------------------------------------------------------
//  <copyright file="StackInfo.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------
using System.Collections.Generic;

namespace Raven.Debug
{
    internal class StackInfo
    {
        public List<uint> ThreadIds = new List<uint>();
        public bool NativeThreads;
        public List<string> StackTrace = new List<string>(); 
    }
}
