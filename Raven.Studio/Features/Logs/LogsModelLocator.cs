// -----------------------------------------------------------------------
//  <copyright file="LogsModelLocator.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------

using Raven.Client.Connection.Async;
using Raven.Studio.Infrastructure;

namespace Raven.Studio.Features.Logs
{
	public class LogsModelLocator : ModelLocatorBase<LogsModel>
	{
		protected override void Load(IAsyncDatabaseCommands asyncDatabaseCommands, Observable<LogsModel> observable)
		{
			var logsParameters = GetParamAfter("/logs/");
			var showErrorOnly = (logsParameters == null || logsParameters != "errors") == false;
			observable.Value = new LogsModel(asyncDatabaseCommands, showErrorOnly);
		}
	}
}