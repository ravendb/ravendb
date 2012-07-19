// -----------------------------------------------------------------------
//  <copyright file="TaskErrorExtensions.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------
using System;
using System.Diagnostics;
using System.Threading.Tasks;

namespace Raven.Client.Changes
{
	internal static class TaskErrorExtensions
	{
		 public static Task CatchAndIgnore(this Task self)
		 {
			 // this merely observe the exception task, nothing else
			 return self.ContinueWith(task =>
			                          	{
											if(task.IsFaulted)
			                          			GC.KeepAlive(task.Exception);
			                          		return task;
			                          	})
				 .Unwrap();
		 }
	}
}