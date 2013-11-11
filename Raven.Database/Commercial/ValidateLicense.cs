//-----------------------------------------------------------------------
// <copyright file="ValidateLicense.cs" company="Hibernating Rhinos LTD">
//     Copyright (c) Hibernating Rhinos LTD. All rights reserved.
// </copyright>
//-----------------------------------------------------------------------
using System;
using System.Collections.Generic;
using System.ComponentModel.Composition;
using System.ComponentModel.Composition.Hosting;
using System.IO;
using System.Text;
using System.Threading;
using System.Xml;
using Raven.Abstractions.Data;
using Raven.Abstractions.Logging;
using Raven.Database.Config;
using Raven.Database.Impl.Clustering;
using Raven.Database.Plugins;
using Rhino.Licensing;
using Rhino.Licensing.Discovery;
using Raven.Database.Extensions;
using System.Linq;

namespace Raven.Database.Commercial
{
	using Raven.Abstractions;

	internal class ValidateLicense : IDisposable
	{
		public static LicensingStatus CurrentLicense { get; set; }
		public static Dictionary<string, string> LicenseAttributes { get; set; }
		private AbstractLicenseValidator licenseValidator;
		private readonly ILog logger = LogManager.GetCurrentClassLogger();
		private Timer timer;

		private static readonly Dictionary<string,string> alwaysOnAttributes = new Dictionary<string, string>
		{
			{"periodicBackup", "false"},
			{"encryption", "false"},
			{"fips", "false"},
			{"compression", "false"},
			{"quotas","false"},

			{"authorization","true"},
			{"documentExpiration","true"},
			{"replication","true"},
			{"versioning","true"},
		};

		static ValidateLicense()
		{
			CurrentLicense = new LicensingStatus
			{
				Status = "AGPL - Open Source",
				Error = false,
				Message = "No license file was found.\r\n" +
				          "The AGPL license restrictions apply, only Open Source / Development work is permitted.",
				Attributes = new Dictionary<string, string>(alwaysOnAttributes, StringComparer.OrdinalIgnoreCase)
			};
		}

		public void Execute(InMemoryRavenConfiguration config)
		{
			timer = new Timer(state => ExecuteInternal(config), null, TimeSpan.FromHours(1), TimeSpan.FromHours(1));

			ExecuteInternal(config);
		}

		private void ExecuteInternal(InMemoryRavenConfiguration config)
		{
			var licensePath = GetLicensePath(config);
			var licenseText = GetLicenseText(config);
			
			if (TryLoadLicense(licenseText) == false) 
				return;

			try
			{
				licenseValidator.AssertValidLicense(() =>
				{
					string value;

					AssertForV2(licenseValidator.LicenseAttributes);
					if (licenseValidator.LicenseAttributes.TryGetValue("OEM", out value) &&
					    "true".Equals(value, StringComparison.OrdinalIgnoreCase))
					{
						licenseValidator.MultipleLicenseUsageBehavior = AbstractLicenseValidator.MultipleLicenseUsage.AllowSameLicense;
					}
					string allowExternalBundles;
					if(licenseValidator.LicenseAttributes.TryGetValue("allowExternalBundles", out allowExternalBundles) && 
						bool.Parse(allowExternalBundles) == false)
					{
						var directoryCatalogs = config.Catalog.Catalogs.OfType<DirectoryCatalog>().ToArray();
						foreach (var catalog in directoryCatalogs)
						{
							config.Catalog.Catalogs.Remove(catalog);
						}
					}
				});

				var attributes = new Dictionary<string, string>(alwaysOnAttributes, StringComparer.OrdinalIgnoreCase);
				foreach (var licenseAttribute in licenseValidator.LicenseAttributes)
				{
					attributes[licenseAttribute.Key] = licenseAttribute.Value;
				}

				var message = "Valid license at " + licensePath;
				var status = "Commercial - " + licenseValidator.LicenseType;

				if (licenseValidator.IsOemLicense() && licenseValidator.ExpirationDate < SystemTime.UtcNow)
				{
					message = string.Format("Expired ({0}) OEM license at {1}", licenseValidator.ExpirationDate.ToShortDateString(), licensePath);
					status += " (Expired)";
				}

				CurrentLicense = new LicensingStatus
				{
					Status = status,
					Error = false,
					Message = message,
					Attributes = attributes
				};
			}
			catch (Exception e)
			{
				logger.ErrorException("Could not validate license at " + licensePath + ", " + licenseText, e);

				try
				{
					var xmlDocument = new XmlDocument();
					xmlDocument.Load(licensePath);
					var ns = new XmlNamespaceManager(xmlDocument.NameTable);
					ns.AddNamespace("sig", "http://www.w3.org/2000/09/xmldsig#");
					var sig = xmlDocument.SelectSingleNode("/license/sig:Signature",ns);
					if (sig != null)
					{
						sig.RemoveAll();
					}
					licenseText = xmlDocument.InnerXml;
				}
				catch (Exception)
				{
					// couldn't remove the signature, maybe not XML?
				}

				CurrentLicense = new LicensingStatus
				{
					Status = "AGPL - Open Source",
					Error = true,
					Message = "Could not validate license: " + licensePath + ", " + licenseText + Environment.NewLine + e,
					Attributes = new Dictionary<string, string>(alwaysOnAttributes, StringComparer.OrdinalIgnoreCase)
				};
			}
		}

		private bool TryLoadLicense(string licenseText)
		{
			string publicKey;
			using (
				var stream = typeof (ValidateLicense).Assembly.GetManifestResourceStream("Raven.Database.Commercial.RavenDB.public"))
			{
				if (stream == null)
					throw new InvalidOperationException("Could not find public key for the license");
				publicKey = new StreamReader(stream).ReadToEnd();
			}


			licenseValidator = new StringLicenseValidator(publicKey, licenseText)
			{
				DisableFloatingLicenses = true,
				SubscriptionEndpoint = "http://uberprof.com/Subscriptions.svc"
			};
			licenseValidator.LicenseInvalidated += OnLicenseInvalidated;
			licenseValidator.MultipleLicensesWereDiscovered += OnMultipleLicensesWereDiscovered;

			if (string.IsNullOrEmpty(licenseText))
			{
				CurrentLicense = new LicensingStatus
				{
					Status = "AGPL - Open Source",
					Error = false,
					Message = "No license file was found at " + licenseText +
					          "\r\nThe AGPL license restrictions apply, only Open Source / Development work is permitted."
				};
				return false;
			}
			return true;
		}

		private void AssertForV2(IDictionary<string, string> licenseAttributes)
		{
			string version;
			if (licenseAttributes.TryGetValue("version", out version) == false)
			{
				if (licenseValidator.LicenseType != LicenseType.Subscription)
				throw new LicenseExpiredException("This is not a license for RavenDB 2.0");

				// Add backward compatibility for the subscription licenses of v1
				licenseAttributes["version"]= "2.5";
				licenseAttributes["implicit20StandardLicenseBy10Subscription"]= "true";
				licenseAttributes["allowWindowsClustering"]= "false";
				licenseAttributes["numberOfDatabases"]= "unlimited";
				licenseAttributes["periodicBackup"]= "true";
				licenseAttributes["encryption"]= "false";
				licenseAttributes["compression"]= "false";
				licenseAttributes["quotas"]= "false";
				licenseAttributes["authorization"]= "true";
				licenseAttributes["documentExpiration"]= "true";
				licenseAttributes["replication"]= "true";
				licenseAttributes["versioning"]= "true";
				licenseAttributes["maxSizeInMb"]= "unlimited";

				string oem;
				if (licenseValidator.LicenseAttributes.TryGetValue("OEM", out oem) &&
				    "true".Equals(oem ,StringComparison.OrdinalIgnoreCase))
				{
					licenseAttributes["OEM"]= "true";
					licenseAttributes["maxRamUtilization"]= "6442450944";
					licenseAttributes["maxParallelism"]= "3";

				}
				else
				{
					licenseAttributes["OEM"]= "false";
					licenseAttributes["maxRamUtilization"]= "12884901888";
					licenseAttributes["maxParallelism"]= "6";
				}                     
			}
			else
			{
				if (version != "1.2" && version != "2.0" && version != "2.5")
					throw new LicenseExpiredException("This is not a license for RavenDB 2.x");
			}

			string maxRam;
			if (licenseAttributes.TryGetValue("maxRamUtilization", out maxRam))
			{
				if (string.Equals(maxRam, "unlimited",StringComparison.OrdinalIgnoreCase) == false)
				{
					MemoryStatistics.MemoryLimit = (int)(long.Parse(maxRam) / 1024 / 1024);
				}
			}
			
			string maxParallel;
			if (licenseAttributes.TryGetValue("maxParallelism", out maxParallel))
			{
				if (string.Equals(maxParallel, "unlimited", StringComparison.OrdinalIgnoreCase) == false)
				{
					MemoryStatistics.MaxParallelism = int.Parse(maxParallel);
				}
			}

			var clasterInspector = new ClusterInspecter();

			string claster;
			if (licenseAttributes.TryGetValue("allowWindowsClustering", out claster))
			{
				if(bool.Parse(claster) == false)
				{
					if (clasterInspector.IsRavenRunningAsClusterGenericService())
						throw new InvalidOperationException("Your license does not allow clustering, but RavenDB is running in clustered mode");
				}
			}
			else
			{
				if (clasterInspector.IsRavenRunningAsClusterGenericService())
					throw new InvalidOperationException("Your license does not allow clustering, but RavenDB is running in clustered mode");
			}
		}

		[Import(AllowDefault = true)]
		private ILicenseProvider LicenseProvider { get; set; }

		private string GetLicenseText(InMemoryRavenConfiguration config)
		{
			config.Container.SatisfyImportsOnce(this);
			if (LicenseProvider != null && !string.IsNullOrEmpty(LicenseProvider.License))
				return LicenseProvider.License;

			var value = config.Settings["Raven/License"];
			if (string.IsNullOrEmpty(value) == false)
				return value;
			var fullPath = GetLicensePath(config).ToFullPath();
			if (File.Exists(fullPath))
				return File.ReadAllText(fullPath);
			return string.Empty;
		}

		private static string GetLicensePath(InMemoryRavenConfiguration config)
		{
			var value = config.Settings["Raven/License"];
			if (string.IsNullOrEmpty(value) == false)
				return "configuration";
			value = config.Settings["Raven/LicensePath"];
			if (string.IsNullOrEmpty(value) == false)
				return value;
			return Path.Combine(AppDomain.CurrentDomain.BaseDirectory, "license.xml");
		}

		private void OnMultipleLicensesWereDiscovered(object sender, DiscoveryHost.ClientDiscoveredEventArgs clientDiscoveredEventArgs)
		{
			logger.Error("A duplicate license was found at {0} for user {1}. User Id: {2}. Both licenses were disabled!", 
				clientDiscoveredEventArgs.MachineName, 
				clientDiscoveredEventArgs.UserName, 
				clientDiscoveredEventArgs.UserId);

			CurrentLicense = new LicensingStatus
			{
				Status = "AGPL - Open Source",
				Error = true,
				Message =
					string.Format("A duplicate license was found at {0} for user {1}. User Id: {2}.", clientDiscoveredEventArgs.MachineName,
					              clientDiscoveredEventArgs.UserName,
					              clientDiscoveredEventArgs.UserId)
			};
		}

		private void OnLicenseInvalidated(InvalidationType invalidationType)
		{
			logger.Error("The license have expired and can no longer be used");
			CurrentLicense = new LicensingStatus
			{
				Status = "AGPL - Open Source",
				Error = true,
				Message = "License expired"
			};
		}

		public void Dispose()
		{
			if (timer != null)
				timer.Dispose();
		}
	}
}
