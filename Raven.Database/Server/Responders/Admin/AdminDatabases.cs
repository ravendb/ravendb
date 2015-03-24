// -----------------------------------------------------------------------
//  <copyright file="AdminDatabases.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------
using System;
using System.Collections.Generic;
using System.IO;
using System.Security.Cryptography;
using System.Text;
using Raven.Abstractions.Data;
using Raven.Database.Server.Abstractions;
using Raven.Database.Extensions;
using Raven.Imports.Newtonsoft.Json;
using Raven.Json.Linq;
using System.Linq;
using Raven.Abstractions.Extensions;

namespace Raven.Database.Server.Responders.Admin
{
	public class AdminDatabases : AdminResponder
	{
		public override string[] SupportedVerbs
		{
			get { return new[] {"GET", "PUT", "DELETE"}; }
		}

		public override string UrlPattern
		{
			get { return "^/admin/databases/(.+)"; }
		}

		public override void RespondToAdmin(IHttpContext context)
		{
			if (EnsureSystemDatabase(context) == false)
				return;

			var match = urlMatcher.Match(context.GetRequestUrl());
			var db = Uri.UnescapeDataString(match.Groups[1].Value);
			
			DatabaseDocument dbDoc;
			var docKey = "Raven/Databases/" + db;
			switch (context.Request.HttpMethod)
			{
				case "GET":
					if (db.Equals(Constants.SystemDatabase,StringComparison.OrdinalIgnoreCase))
					{
						//fetch fake (empty) system database document
						var systemDatabaseDocument = new DatabaseDocument { Id = Constants.SystemDatabase };
						var serializedDatabaseDocument = RavenJObject.FromObject(systemDatabaseDocument);

						context.WriteJson(serializedDatabaseDocument);
					}
					else
					{
						dbDoc = GetDatabaseDocument(context, docKey, db);
						context.WriteJson(dbDoc);
					}

					break;
				case "PUT":
					if (!db.Equals(Constants.SystemDatabase, StringComparison.OrdinalIgnoreCase))
					{
						try
						{
							dbDoc = context.ReadJsonObject<DatabaseDocument>();
						}
						catch (InvalidOperationException e)
						{
							context.SetSerializationException(e);
							return;
						}
						catch (InvalidDataException e)
						{
							context.SetSerializationException(e);
							return;
						}

						server.Protect(dbDoc);
						var json = RavenJObject.FromObject(dbDoc);
						json.Remove("Id");

						Database.Put(docKey, null, json, new RavenJObject(), null);
					}
					else
					{
						context.SetStatusToForbidden(); //forbidden to edit system database document
					}
					break;
				case "DELETE":
					if (!db.Equals(Constants.SystemDatabase, StringComparison.OrdinalIgnoreCase))
					{
						var configuration = server.CreateTenantConfiguration(db);
						var databasedocument = Database.Get(docKey, null);

						if (configuration == null)
							return;
						Database.Delete(docKey, null, null);
						bool result;
						if (bool.TryParse(context.Request.QueryString["hard-delete"], out result) && result)
						{
							IOExtensions.DeleteDirectory(configuration.DataDirectory);
							IOExtensions.DeleteDirectory(configuration.IndexStoragePath);

							if (databasedocument != null)
							{
								dbDoc = databasedocument.DataAsJson.JsonDeserialization<DatabaseDocument>();
								if (dbDoc != null && dbDoc.Settings.ContainsKey(Constants.RavenLogsPath))
									IOExtensions.DeleteDirectory(dbDoc.Settings[Constants.RavenLogsPath]);
							}
						}
					}
					else
					{
						context.SetStatusToForbidden(); //forbidden to delete system database document
					}
					break;
			}
		}

		private DatabaseDocument GetDatabaseDocument(IHttpContext context, string docKey, string db)
		{
			var document = Database.Get(docKey, null);
			if (document == null)
			{
				context.SetStatusToNotFound();
				return null;
			}

			var dbDoc = document.DataAsJson.JsonDeserialization<DatabaseDocument>();
			dbDoc.Id = db;
			server.Unprotect(dbDoc);
			return dbDoc;
		}
	}
}