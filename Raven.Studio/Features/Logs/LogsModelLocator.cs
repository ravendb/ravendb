// -----------------------------------------------------------------------
//  <copyright file="LogsModelLocator.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------

using Raven.Client.Connection.Async;
using Raven.Studio.Infrastructure;
using Raven.Studio.Models;

namespace Raven.Studio.Features.Logs
{
	public class LogsModelLocator : ModelLocatorBase<LogsModel>
	{
		protected override void Load(DatabaseModel database, IAsyncDatabaseCommands asyncDatabaseCommands, Observable<LogsModel> observable)
		{
			var logsParameters = GetParamAfter("/logs/");
			var showErrorsOnly = (logsParameters == null || logsParameters.StartsWith("error") == false) == false;
			observable.Value = new LogsModel(asyncDatabaseCommands, showErrorsOnly);
		}
	}
}