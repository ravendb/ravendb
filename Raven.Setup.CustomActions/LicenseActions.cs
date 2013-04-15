using System;
using System.Globalization;
using System.IO;
using System.Reflection;
using System.Security.Cryptography;
using System.Security.Cryptography.Xml;
using System.Xml;

namespace Raven.Setup.CustomActions
{
	using System.Threading;
	using System.Windows.Forms;
	using Microsoft.Deployment.WindowsInstaller;

	public class LicenseActions
	{
		private static string publicKey;

		static LicenseActions()
		{
			using (var stream = Assembly.GetExecutingAssembly().GetManifestResourceStream("Raven.Setup.CustomActions.RavenDB.public"))
			{
				if (stream == null)
					throw new InvalidOperationException("Could not find public key for the license");
				publicKey = new StreamReader(stream).ReadToEnd();
			}
		}

		[CustomAction]
		public static ActionResult OpenLicenseFileChooser(Session session)
		{
			try
			{
				var task = new Thread(() => GetFile(session));
				task.SetApartmentState(ApartmentState.STA);
				task.Start();
				task.Join();
				return ActionResult.Success;
			}
			catch (Exception ex)
			{
				Log.Error(session, "Error occurred during OpenLicenseFileChooser. Exception: " + ex);
				return ActionResult.Failure;
			}
		}

		[CustomAction]
		public static ActionResult LicenseFileExists(Session session)
		{
			try
			{
				var task = new Thread(() =>
				{
					var licensePath = session["RAVEN_LICENSE_FILE_PATH"];

					CleanUpLicenseInfo(session);

					if (string.IsNullOrEmpty(licensePath))
					{
						session["RAVEN_LICENSE_VALID"] = "False";
						return;
					}

					var licenseExists = File.Exists(licensePath);

					if (licenseExists)
					{
						Log.Info(session, "Checking existing license file");

						using (var licenseStream = File.Open(licensePath, FileMode.Open))
						{
							var license = new StreamReader(licenseStream).ReadToEnd();
							if (CheckLicense(session, license))
								session["RAVEN_LICENSE_VALID"] = "True";
							else
								session["RAVEN_LICENSE_VALID"] = "False";
						}
					}
					else
					{
						session["RAVEN_LICENSE_ERROR"] = "File does not exists under the specified path";
						session["RAVEN_LICENSE_VALID"] = "False";
					}
				});
				task.SetApartmentState(ApartmentState.STA);
				task.Start();
				task.Join();

				return ActionResult.Success;
			}
			catch (Exception ex)
			{
				Log.Error(session, "Error occurred during OpenLicenseFileChooser. Exception: " + ex);
				return ActionResult.Failure;
			}
		}

		private static void GetFile(Session session)
		{
			var fileDialog = new OpenFileDialog { Filter = "License File (*.xml)|*.xml" };
			if (fileDialog.ShowDialog() == DialogResult.OK)
			{
				session["RAVEN_LICENSE_FILE_PATH"] = fileDialog.FileName;
			}
		}

		private static bool CheckLicense(Session session, string license)
		{
			var doc = new XmlDocument();
			try
			{
				doc.LoadXml(license);
			}
			catch (XmlException)
			{
				session["RAVEN_LICENSE_ERROR"] = "Could not understand the license, it isn't a valid XML file";
				return false;
			}

			if (IsSignatureValid(publicKey, doc) == false)
			{
				session["RAVEN_LICENSE_ERROR"] =  "Could not validate xml signature";
				return false;
			}

			if (doc.SelectSingleNode("/floating-license") != null)
			{
				session["RAVEN_LICENSE_TYPE"] = "License type: Floating";
				return true;
			}

			try
			{
				ValidateXmlDocumentLicense(session, doc);
			}
			catch (Exception ex)
			{
				session["RAVEN_LICENSE_ERROR"] = ex.Message;
				return false;
			}

			return true;
		}

		private static bool IsSignatureValid(string licensePublicKey, XmlDocument doc)
		{
			var rsa = new RSACryptoServiceProvider();
			rsa.FromXmlString(licensePublicKey);

			var nsMgr = new XmlNamespaceManager(doc.NameTable);
			nsMgr.AddNamespace("sig", "http://www.w3.org/2000/09/xmldsig#");

			var signedXml = new SignedXml(doc);
			var sig = (XmlElement)doc.SelectSingleNode("//sig:Signature", nsMgr);
			if (sig == null)
			{
				return false;
			}

			signedXml.LoadXml(sig);

			return signedXml.CheckSignature(rsa);
		}

		private static void ValidateXmlDocumentLicense(Session session, XmlDocument doc)
		{
			XmlNode id = doc.SelectSingleNode("/license/@id");
			if (id == null)
			{
				throw new InvalidOperationException("Could not find id attribute in license");
			}

			XmlNode date = doc.SelectSingleNode("/license/@expiration");
			if (date == null)
			{
				throw new InvalidOperationException("Could not find expiration in license");
			}

			var expirationDate = DateTime.ParseExact(date.Value, "yyyy-MM-ddTHH:mm:ss.fffffff", CultureInfo.InvariantCulture);

			session["RAVEN_LICENSE_EXPIRATION"] = "Expiration date: " + expirationDate.ToString("yyyy-MMM-dd");

			XmlNode licenseType = doc.SelectSingleNode("/license/@type");
			if (licenseType == null)
			{
				throw new InvalidOperationException("Could not find license type in license");
			}

			session["RAVEN_LICENSE_TYPE"] = "License type: " + licenseType.Value;

			XmlNode name = doc.SelectSingleNode("/license/name/text()");
			if (name == null)
			{
				throw new InvalidOperationException("Could not find licensee's name");
			}

			session["RAVEN_LICENSE_USER_NAME"] = "Licensed to: " + name.Value;

			var license = doc.SelectSingleNode("/license");

			var numberOfAttributes = 1;

			var attributesColumn1 = string.Empty;
			var attributesColumn2 = string.Empty;

			foreach (XmlAttribute attrib in license.Attributes)
			{
				if (attrib.Name == "type" || attrib.Name == "expiration" || attrib.Name == "id")
					continue;
				numberOfAttributes++;

				var attribute = string.Format("{0}: {1}{2}", ToUpperFirstLetter(attrib.Name), attrib.Value, Environment.NewLine);

				if (numberOfAttributes <= 10)
				{
					attributesColumn1 += attribute;
				}
				else
				{
					attributesColumn2 += attribute;
				}
			}

			session["RAVEN_LICENSE_ATTRIBUTES1"] = attributesColumn1;
			session["RAVEN_LICENSE_ATTRIBUTES2"] = attributesColumn2;
		}

		public static string ToUpperFirstLetter( string source)
		{
			return source.Substring(0, 1).ToUpper() + source.Substring(1);
		}

		private static void CleanUpLicenseInfo(Session session)
		{
			session["RAVEN_LICENSE_VALID"] = string.Empty;
			session["RAVEN_LICENSE_ERROR"] = string.Empty;
			session["RAVEN_LICENSE_TYPE"] = string.Empty;
			session["RAVEN_LICENSE_EXPIRATION"] = string.Empty;
			session["RAVEN_LICENSE_USER_NAME"] = string.Empty;
			session["RAVEN_LICENSE_ATTRIBUTES1"] = string.Empty;
			session["RAVEN_LICENSE_ATTRIBUTES2"] = string.Empty;
		}
	}
}
