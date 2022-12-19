// -----------------------------------------------------------------------
//  <copyright file="StackInfo.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------
using System.Collections.Generic;

namespace Raven.Debug.StackTrace
{
    internal class StackInfo
    {
        public List<int> ThreadIds = new List<int>();
        public List<string> StackTrace = new List<string>();
    }
}
