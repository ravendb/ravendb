using System.ComponentModel.Composition.Hosting;
using System.Reflection;
using Raven.Abstractions.Data;
using Raven.Database.Config;
using Raven.Database.Plugins;
using Raven.Tests.Common;

using Xunit;

namespace Raven.Tests
{
	public class LicenseProviderPluginTest : RavenTest
	{
		public class TestLicenseProvider : ILicenseProvider
		{
			// We will test an invalid license.  We are just making sure it can be picked up - not its validity.
			public string License
			{
				get { return "<license></license>"; }
			}
		}

		protected override void ModifyConfiguration(InMemoryRavenConfiguration configuration)
		{
			configuration.Catalog.Catalogs.Add(new TypeCatalog(typeof(TestLicenseProvider)));
		}

		[Fact]
		public void License_Can_Be_Provided_Via_Plugin()
		{
			using (var documentStore = NewDocumentStore())
			{
				// We have to get the license status via reflection since it is not exposed otherwise.

				var database = documentStore.DocumentDatabase;

				var field = database.GetType().GetField("initializer", BindingFlags.Instance | BindingFlags.NonPublic);
				Assert.NotNull(field);
				var licField = field.FieldType.GetField("validateLicense", BindingFlags.Instance | BindingFlags.NonPublic);
				Assert.NotNull(licField);
				var validateLicense = licField.GetValue(field.GetValue(database));

				var prop = validateLicense.GetType().GetProperty("CurrentLicense", BindingFlags.Static | BindingFlags.Public);
				Assert.NotNull(prop);
				if (prop == null) return;
				var status = (LicensingStatus) prop.GetValue(validateLicense, null);

				// There should be an error status, since we provided an invalid license.
				// If the license wasn't picked up, it would use the AGPL license with no error.
				Assert.True(status.Error);
			}
		}
	}
}
