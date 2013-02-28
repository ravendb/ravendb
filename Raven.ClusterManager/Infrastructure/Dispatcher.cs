// -----------------------------------------------------------------------
//  <copyright file="TaskDispatcher.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------
using System.Threading.Tasks;

namespace Raven.ClusterManager.Infrastructure
{
	public static class Dispatcher
	{
		 public static void HandleResult(Task task)
		 {
			 // TODO: Log that it happened, and errors if they happened
		 }
	}
}