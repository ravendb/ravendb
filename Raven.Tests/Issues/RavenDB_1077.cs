// -----------------------------------------------------------------------
//  <copyright file="RavenDB_1077.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------
using System.Linq;
using System.Reflection;
using Raven.Abstractions.Data;
using Raven.Database.Commercial;
using Raven.Database.Storage;
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

				var field = database.GetType().GetField("validateLicense", BindingFlags.Instance | BindingFlags.NonPublic);
				Assert.NotNull(field);
				var validateLicense = field.GetValue(database);

				var prop = validateLicense.GetType().GetProperty("CurrentLicense", BindingFlags.Static | BindingFlags.Public);
				Assert.NotNull(prop);
				var status = (LicensingStatus)prop.GetValue(validateLicense, null);

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

				var field = database.GetType().GetField("validateLicense", BindingFlags.Instance | BindingFlags.NonPublic);
				var validateLicense = field.GetValue(database);

				var currentLicenseProp = validateLicense.GetType().GetProperty("CurrentLicense", BindingFlags.Static | BindingFlags.Public);

				var status = (LicensingStatus)currentLicenseProp.GetValue(validateLicense, null);

				status.Error = false;
				status.Status = "Commercial";

				var licenseDetectorTask = database.StartupTasks.OfType<CommercialLicenseDetector>().First();

				licenseDetectorTask.Execute(database);

				ListItem validCommercialLicense = null;
				database.TransactionalStorage.Batch(
					accessor => validCommercialLicense = accessor.Lists.Read("Raven/License/Commercial", "ValidLicense"));

				Assert.NotNull(validCommercialLicense);
			}
		}

		[Fact]
		public void ShouldDetectThatValidLicenseWasProvidedInThePast()
		{
			using (var documentStore = NewDocumentStore())
			{
				var database = documentStore.DocumentDatabase;

				var field = database.GetType().GetField("validateLicense", BindingFlags.Instance | BindingFlags.NonPublic);
				var validateLicense = field.GetValue(database);

				var currentLicenseProp = validateLicense.GetType().GetProperty("CurrentLicense", BindingFlags.Static | BindingFlags.Public);

				var status = (LicensingStatus)currentLicenseProp.GetValue(validateLicense, null);

				status.Error = false;
				status.Status = "Commercial";

				var licenseDetectorTask = database.StartupTasks.OfType<CommercialLicenseDetector>().First();

				licenseDetectorTask.Execute(database); // this will store info in database

				// reset license info - mark it as invalid
				status.Error = true;
				status.Status = "";
				status.ValidCommercialLicenseSeen = false;

				licenseDetectorTask.Execute(database); // this should notice that in the database there is a valid license marker under "Raven/License/Commercial"

				Assert.True(status.ValidCommercialLicenseSeen);
			}
		} 
	}
}