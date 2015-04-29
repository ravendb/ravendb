// -----------------------------------------------------------------------
//  <copyright file="ServerValidation.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------
using System;
using System.Net;
using Raven.Abstractions.Connection;
using Raven.Abstractions.Data;
using Raven.Abstractions.Exceptions;
using Raven.Client.Document;

namespace Raven.Client.Smuggler
{
	public class ServerValidation
	{
		public static void ValidateThatServerIsUpAndDatabaseExists(RavenConnectionStringOptions server, DocumentStore s)
		{
			var shouldDispose = false;

			try
			{
				var commands = !string.IsNullOrEmpty(server.DefaultDatabase)
								   ? s.DatabaseCommands.ForDatabase(server.DefaultDatabase)
								   : s.DatabaseCommands;

				commands.GetStatistics(); // check if database exist
			}
			catch (Exception e)
			{
				shouldDispose = true;

				var responseException = e as ErrorResponseException;
				if (responseException != null && responseException.StatusCode == HttpStatusCode.ServiceUnavailable && responseException.Message.StartsWith("Could not find a database named"))
					throw new SmugglerException(
						string.Format(
							"Smuggler does not support database creation (database '{0}' on server '{1}' must exist before running Smuggler).",
							server.DefaultDatabase,
							s.Url), e);


				if (e.InnerException != null)
				{
					var webException = e.InnerException as WebException;
					if (webException != null)
					{
						throw new SmugglerException(string.Format("Smuggler encountered a connection problem: '{0}'.", webException.Message), webException);
					}
				} throw new SmugglerException(string.Format("Smuggler encountered a connection problem: '{0}'.", e.Message), e);
			}
			finally
			{
				if (shouldDispose)
					s.Dispose();
			}
		}
	}
}