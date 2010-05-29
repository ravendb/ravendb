#if COMMERCIAL
using System;
using System.IO;
using Newtonsoft.Json.Linq;
using Rhino.Licensing;
using System.Linq;
using Raven.Database.Json;

namespace Raven.Database.Commercial
{
	public class ValidateLicense : IStartupTask
	{
		private DocumentDatabase docDb;
		private LicenseValidator licenseValidator;

		public void Execute(DocumentDatabase database)
		{
			docDb = database;
			string publicKey;
			using(var stream = typeof(ValidateLicense).Assembly.GetManifestResourceStream("Raven.Database.Commercial.RavenDB.public"))
			{
				if(stream == null)
					throw new InvalidOperationException("Could not find public key for the license");
				publicKey = new StreamReader(stream).ReadToEnd();
			}
			var fullPath = Path.GetFullPath(Path.Combine(AppDomain.CurrentDomain.BaseDirectory, "license.xml"));
			licenseValidator = new LicenseValidator(publicKey, fullPath)
			{
				DisableFloatingLicenses = true,
			};
			licenseValidator.LicenseInvalidated+=LicenseValidatorOnLicenseInvalidated;
			if (licenseValidator.TryLoadingLicenseValuesFromValidatedXml() == false)
				throw new LicenseNotFoundException("Could not find valid license for RavenDB at: " + fullPath);
			
			if (DateTime.UtcNow < licenseValidator.ExpirationDate)
				return;

			LicenseValidatorOnLicenseInvalidated(InvalidationType.TimeExpired);
		}

		private void LicenseValidatorOnLicenseInvalidated(InvalidationType invalidationType)
		{
			var document = docDb.Get("Raven/WarningMessages", null);
			WarningMessagesHolder messagesHolder = document == null ?
				new WarningMessagesHolder() :
				document.DataAsJson.JsonDeserialization<WarningMessagesHolder>();

			if (messagesHolder.Messages.Any(warnMsg => warnMsg.StartsWith("Licensing:")) == false)
			{
				messagesHolder.Messages.Add("Licensing: RavenDB license has expired at " +
					licenseValidator.ExpirationDate.ToShortDateString());
			}
			docDb.Put("Raven/WarningMessages", null,
						 JObject.FromObject(messagesHolder), 
						 new JObject(), null);
		}
	}
}
#endif