// -----------------------------------------------------------------------
//  <copyright file="RavenDB_1077.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------
using System.Collections.Generic;
using System.Linq;
using Raven.Abstractions.Data;
using Raven.Abstractions.Exceptions;
using Raven.Client.Embedded;
using Raven.Database.Server.Security;
using Raven.Database.Server.Security.Windows;
using Raven.Database.Storage;
using Raven.Json.Linq;
using Xunit;

namespace Raven.Tests.Issues
{
	public class RavenDB_1077 : RavenTest
	{
		[Fact]
		public void ShouldNotStoreInfoAbouValidCommercialLicense_WhenLicenseWasNotProvided()
		{
			using (var documentStore = NewDocumentStore())
			{
				var database = documentStore.DocumentDatabase;

				var status = GetLicenseByReflection(database);

				Assert.False(status.ValidCommercialLicenseSeen);

				ListItem validCommercialLicense = null;
				database.TransactionalStorage.Batch(
					accessor => validCommercialLicense = accessor.Lists.Read("Raven/License/Commercial", "ValidLicense"));

				Assert.Null(validCommercialLicense);
			}
		}

		[Fact]
		public void ShouldStoreInfoAbouValidCommercialLicense_WhenLicenseWasProvided()
		{
			using (var documentStore = NewDocumentStore())
			{
				var database = documentStore.DocumentDatabase;

				var status = GetLicenseByReflection(database);

				status.Error = false;
				status.Status = "Commercial";

				var task = database.StartupTasks.OfType<AuthenticationForCommercialUseOnly>().First();

				task.Execute(database);

				ListItem validCommercialLicense = null;
				database.TransactionalStorage.Batch(
					accessor => validCommercialLicense = accessor.Lists.Read("Raven/License/Commercial", "ValidLicense"));

				Assert.NotNull(validCommercialLicense);
			}
		}

		[Fact]
		public void ShouldDetectThatValidLicenseWasProvidedInThePast()
		{
			using (var documentStore = NewDocumentStore(enableAuthentication: true)) // / this will store info in database
			{
				var database = documentStore.DocumentDatabase;
				var status = GetLicenseByReflection(database);

				// reset license info - mark it as invalid
				status.Error = true;
				status.Status = "";
				status.ValidCommercialLicenseSeen = false;

				var task = database.StartupTasks.OfType<AuthenticationForCommercialUseOnly>().First();

				task.Execute(database); // this should notice that in the database there is a valid license marker under "Raven/License/Commercial"

				Assert.True(status.ValidCommercialLicenseSeen);
			}
		}

		[Fact]
		public void ShouldRefuseToSetupWindowsAuth_WithoutValidCommercialLicense()
		{
			using (var documentStore = NewDocumentStore())
			{
				var exception = Assert.Throws<OperationVetoedException>(() =>
				{
					SetupWindowsAuth(documentStore);
				});

				Assert.Contains("Cannot setup Windows Authentication without a valid commercial license.", exception.Message);
			}
		}

		[Fact]
		public void ShouldSetupOAuthSuccessfully_WithoutValidCommercialLicense()
		{
			using (var documentStore = NewDocumentStore())
			{
				var exception = Assert.Throws<OperationVetoedException>(() =>
				{
					SetupOAuth(documentStore);
				});

				Assert.Contains("Cannot setup OAuth Authentication without a valid commercial license.", exception.Message);
			}
		}

		[Fact]
		public void ShouldAllowToSetupWindowsAuth_WhenValidCommercialLicenseProvided()
		{
			using (var documentStore = NewDocumentStore(enableAuthentication: true))
			{
				Assert.DoesNotThrow(() =>
				{
					SetupWindowsAuth(documentStore);
				});
			}
		}

		[Fact]
		public void ShouldAllowToSetupOAuth_WhenValidCommercialLicensePrivided()
		{
			using (var documentStore = NewDocumentStore(enableAuthentication: true))
			{
				Assert.DoesNotThrow(() =>
				{
					SetupOAuth(documentStore);
				});
			}
		}

		private static void SetupWindowsAuth(EmbeddableDocumentStore documentStore)
		{
			documentStore.DatabaseCommands.Put("Raven/Authorization/WindowsSettings", null,
											   RavenJObject.FromObject(new WindowsAuthDocument()
											   {
												   RequiredUsers = new List<WindowsAuthData>()
				                                   {
					                                   new WindowsAuthData()
					                                   {
						                                   Name = "test",
						                                   Enabled = true,
						                                   Databases = new List<DatabaseAccess>
						                                   {
							                                   new DatabaseAccess {TenantId = "<system>"},
						                                   }
					                                   }
				                                   }
											   }), new RavenJObject());
		}

		private static void SetupOAuth(EmbeddableDocumentStore documentStore)
		{
			documentStore.DatabaseCommands.Put("Raven/ApiKeys/test", null, RavenJObject.FromObject(new ApiKeyDefinition
			{
				Name = "test",
				Secret = "test",
				Enabled = true,
				Databases = new List<DatabaseAccess>
				{
					new DatabaseAccess {TenantId = "<system>"},
				}
			}), new RavenJObject());
		}
	}
}