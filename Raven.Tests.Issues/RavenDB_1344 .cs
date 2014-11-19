// -----------------------------------------------------------------------
//  <copyright file="RavenDB_1344 .cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------
using System.Collections.Generic;
using Raven.Abstractions.Data;
using Raven.Abstractions.Exceptions;
using Raven.Json.Linq;
using Raven.Tests.Common;
using Xunit;

namespace Raven.Tests.Issues
{
	public class RavenDB_1344 : RavenTest
	{
		[Fact]
		public void ShouldPreventFromChangingActiveBundles()
		{
			using (var store = NewDocumentStore())
			{
				var dbDoc = new DatabaseDocument()
				{
					Settings = new Dictionary<string, string>()
					{
						{
							Constants.ActiveBundles, ""
						}
					}
				};

				store.SystemDatabase.Documents.Put("Raven/Databases/RavenDB_1344_1", null, RavenJObject.FromObject(dbDoc), new RavenJObject(), null);

				dbDoc.Settings[Constants.ActiveBundles] = "Replication";

				Assert.Throws<OperationVetoedException>(() => store.SystemDatabase.Documents.Put("Raven/Databases/RavenDB_1344_1", null, RavenJObject.FromObject(dbDoc), new RavenJObject(), null));


				var dbDoc2 = new DatabaseDocument()
				{
					Settings = new Dictionary<string, string>()
					{
						{
							Constants.ActiveBundles, "Replication"
						}
					}
				};

				store.SystemDatabase.Documents.Put("Raven/Databases/RavenDB_1344_2", null, RavenJObject.FromObject(dbDoc2), new RavenJObject(), null);

				dbDoc2.Settings[Constants.ActiveBundles] = "";

				Assert.Throws<OperationVetoedException>(() => store.SystemDatabase.Documents.Put("Raven/Databases/RavenDB_1344_2", null, RavenJObject.FromObject(dbDoc2), new RavenJObject(), null));

				var dbDoc3 = new DatabaseDocument()
				{
					Settings = new Dictionary<string, string>()
					{
						{
							Constants.ActiveBundles, "Replication"
						}
					}
				};

				store.SystemDatabase.Documents.Put("Raven/Databases/RavenDB_1344_3", null, RavenJObject.FromObject(dbDoc3), new RavenJObject(), null);

				dbDoc3.Settings[Constants.ActiveBundles] = "SqlReplication";

				Assert.Throws<OperationVetoedException>(() => store.SystemDatabase.Documents.Put("Raven/Databases/RavenDB_1344_3", null, RavenJObject.FromObject(dbDoc3), new RavenJObject(), null));
			}
		}

		[Fact]
		public void ShouldAllowToChangeActiveBundles_IfSpecialMetadataSpecified()
		{
			using (var store = NewDocumentStore())
			{
				var dbDoc = new DatabaseDocument()
				{
					Settings = new Dictionary<string, string>()
					{
						{
							Constants.ActiveBundles, ""
						}
					}
				};

				store.SystemDatabase.Documents.Put("Raven/Databases/RavenDB_1344_1", null, RavenJObject.FromObject(dbDoc), new RavenJObject(), null);

				dbDoc.Settings[Constants.ActiveBundles] = "Replication";

				Assert.DoesNotThrow(() => store.SystemDatabase.Documents.Put("Raven/Databases/RavenDB_1344_1", null, RavenJObject.FromObject(dbDoc), new RavenJObject()
				{
					{
						Constants.AllowBundlesChange, "true"
					}
				}, null));


				var dbDoc2 = new DatabaseDocument()
				{
					Settings = new Dictionary<string, string>()
					{
						{
							Constants.ActiveBundles, "Replication"
						}
					}
				};

				store.SystemDatabase.Documents.Put("Raven/Databases/RavenDB_1344_2", null, RavenJObject.FromObject(dbDoc2), new RavenJObject(), null);

				dbDoc2.Settings[Constants.ActiveBundles] = "";

				Assert.DoesNotThrow(() => store.SystemDatabase.Documents.Put("Raven/Databases/RavenDB_1344_2", null, RavenJObject.FromObject(dbDoc2), new RavenJObject()
				{
					{
						Constants.AllowBundlesChange, "true"
					}
				}, null));

				var dbDoc3 = new DatabaseDocument()
				{
					Settings = new Dictionary<string, string>()
					{
						{
							Constants.ActiveBundles, "Replication"
						}
					}
				};

				store.SystemDatabase.Documents.Put("Raven/Databases/RavenDB_1344_3", null, RavenJObject.FromObject(dbDoc3), new RavenJObject(), null);

				dbDoc3.Settings[Constants.ActiveBundles] = "SqlReplication";

				Assert.DoesNotThrow(() => store.SystemDatabase.Documents.Put("Raven/Databases/RavenDB_1344_3", null, RavenJObject.FromObject(dbDoc3), new RavenJObject()
				{
					{
						Constants.AllowBundlesChange, "true"
					}
				}, null));
			}
		}
	}
}