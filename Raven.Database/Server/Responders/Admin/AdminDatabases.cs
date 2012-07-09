// -----------------------------------------------------------------------
//  <copyright file="AdminDatabases.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------
using System;
using System.Security.Cryptography;
using System.Text;
using Raven.Abstractions.Data;
using Raven.Database.Server.Abstractions;
using Raven.Database.Extensions;
using Raven.Json.Linq;
using System.Linq;

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

			switch (context.Request.HttpMethod)
			{
				case "GET":
					var document = Database.Get("Raven/Databases/" + db, null);
					if(document == null)
					{
						context.SetStatusToNotFound();
						return;
					}
					var securedSettings = document.DataAsJson.Value<RavenJObject>("SecuredSettings");
					if(securedSettings != null)
					{
						foreach (var securedSetting in securedSettings.Select(x=>x.Key).ToList())
						{
							var value = securedSettings.Value<string>(securedSetting);
							var data = Convert.FromBase64String(value);
							var entropy = Encoding.UTF8.GetBytes(securedSetting);
							var unprotected = ProtectedData.Unprotect(data, entropy, DataProtectionScope.CurrentUser);
							securedSettings[securedSetting] = Encoding.UTF8.GetString(unprotected);
						}
					}
					context.WriteJson(document.DataAsJson);
					break;
				case "PUT":
					var dbDoc = context.ReadJsonObject<DatabaseDocument>();
					if(dbDoc.SecuredSettings != null)
					{
						foreach (var prop in dbDoc.SecuredSettings.ToList())
						{
							var bytes = Encoding.UTF8.GetBytes(prop.Value);
							var entrophy = Encoding.UTF8.GetBytes(prop.Key);
							var protectedValue = ProtectedData.Protect(bytes, entrophy, DataProtectionScope.CurrentUser);
							dbDoc.SecuredSettings[prop.Key] = Convert.ToBase64String(protectedValue);
						}
					}
					var json = RavenJObject.FromObject(dbDoc);
					json.Remove("Id");

					Database.Put("Raven/Databases/" + db, null, json, new RavenJObject(), null);
					break;
				case "DELETE":
					Database.Delete("Raven/Databases/" + db, null, null);
					break;
			}
		}
	}
}