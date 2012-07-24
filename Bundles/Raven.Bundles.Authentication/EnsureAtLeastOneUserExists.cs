using System;
using System.IO;
using Raven.Imports.Newtonsoft.Json;
using NLog;
using Raven.Abstractions.Data;
using Raven.Abstractions.Extensions;
using Raven.Database;
using Raven.Database.Plugins;
using Raven.Json.Linq;

namespace Raven.Bundles.Authentication
{
	public class EnsureAtLeastOneUserExists : IStartupTask
	{
		private Logger logger = LogManager.GetCurrentClassLogger();

		public void Execute(DocumentDatabase database)
		{
			if (string.IsNullOrEmpty(database.Name) == false)
				return;// we don't care about tenant databases

			if (string.Equals(database.Configuration.AuthenticationMode, "OAuth", StringComparison.InvariantCultureIgnoreCase) == false)
				return; // we don't care if we aren't using oauth

			var array = database.GetDocumentsWithIdStartingWith("Raven/Users/", null, 0, 1);
			if (array.Length > 0)
				return; // there is already at least one user in there

			var pwd = Guid.NewGuid().ToString();

			if (database.Configuration.RunInMemory == false)
			{
				var authConfigPath = Path.Combine(Path.GetDirectoryName(AppDomain.CurrentDomain.SetupInformation.ConfigurationFile), "authentication.config");

				File.WriteAllText(authConfigPath,
@"Since no users were found in the database, and the database authentication mode was set to OAuth, the following user was automatically created.

Username: Admin
Password: " + pwd + @"

You can use those credentials to login to RavenDB.");

				logger.Info(@"Since no users were found, and the database authentication mode was set to OAuth, a default user was generated name 'Admin'.
Credentials for this user can be found in the following file: {0}", authConfigPath);
			}


			var ravenJTokenWriter = new RavenJTokenWriter();
			JsonExtensions.CreateDefaultJsonSerializer().Serialize(ravenJTokenWriter, new AuthenticationUser
			{
				AllowedDatabases = new[] { "*" },
				Name = "Admin",
				Admin = true
			}.SetPassword(pwd));


			var userDoc = (RavenJObject)ravenJTokenWriter.Token;
			userDoc.Remove("Id");
			database.Put("Raven/Users/Admin", null,
						 userDoc, 
						 new RavenJObject
						{
							{Constants.RavenEntityName, "AuthenticationUsers"},
							{
								Constants.RavenClrType,
								typeof (AuthenticationUser).FullName + ", " + typeof (AuthenticationUser).Assembly.GetName().Name
								}
						}, null);


		}
	}
}