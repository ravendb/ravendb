//-----------------------------------------------------------------------
// <copyright file="CurrentOperationContext.cs" company="Hibernating Rhinos LTD">
//     Copyright (c) Hibernating Rhinos LTD. All rights reserved.
// </copyright>
//-----------------------------------------------------------------------

using System;

using Raven.Abstractions.Data;
using Raven.Abstractions.Extensions;

namespace Raven.Abstractions.Logging
{
	public static class LogContext
	{
		private static readonly Raven.Abstractions.Threading.ThreadLocal<string> databaseName = new Raven.Abstractions.Threading.ThreadLocal<string>();

		public static IDisposable WithDatabase(string database)
		{
			var old = databaseName.Value;
			var db = database ?? Constants.SystemDatabase;
			var disposable = LogManager.OpenMappedContext("database", db);
			databaseName.Value = db;

			return new DisposableAction(()=>
			{
				databaseName.Value = old;
				disposable.Dispose();
			});
		}

		public static string DatabaseName
		{
			get
			{
				try
				{
					return databaseName.Value;
				}
				catch (ObjectDisposedException)
				{
					// can happen when logging from finalizers under crash scenario
					return "unknown";
				}
			}
			set
			{
				try
				{
					databaseName.Value = value;
				}
				catch (ObjectDisposedException)
				{
				}
			}
		}
	}
}