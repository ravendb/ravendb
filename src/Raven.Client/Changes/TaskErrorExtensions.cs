// -----------------------------------------------------------------------
//  <copyright file="TaskErrorExtensions.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------
using System;
using System.Threading.Tasks;

namespace Raven.NewClient.Client.Changes
{
    internal static class TaskErrorExtensions
    {
         public static Task ObserveException(this Task self)
         {
             // this merely observe the exception task, nothing else
            self.ContinueWith(task =>
                                {
                                    if (task.IsFaulted)
                                    {
                                        GC.KeepAlive(task.Exception);
                                    }
                                });
            return self;
         }
    }
}
