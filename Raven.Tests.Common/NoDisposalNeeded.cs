// -----------------------------------------------------------------------
//  <copyright file="EmptyDisposable.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------
using System;

namespace Raven.Tests.Common
{
    public abstract class NoDisposalNeeded : IDisposable
    {
        public void Dispose()
        {
            
        }
    }
}