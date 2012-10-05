//-----------------------------------------------------------------------
// <copyright file="CurrentOperationContext.cs" company="Hibernating Rhinos LTD">
//     Copyright (c) Hibernating Rhinos LTD. All rights reserved.
// </copyright>
//-----------------------------------------------------------------------

using System.Threading;

namespace Raven.Database.Server
{
	public static class LogContext
	{
		public static readonly ThreadLocal<string> DatabaseName = new ThreadLocal<string>();
	}
}