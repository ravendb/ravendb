using System;
using System.Collections.Generic;
using System.Globalization;
using System.Net.NetworkInformation;
using System.Security.Cryptography;
using System.Security.Cryptography.Xml;
using System.ServiceModel;
using System.Threading;
using System.Xml;
using NLog;
using Rhino.Licensing.Discovery;

namespace Rhino.Licensing
{
	/// <summary>
	/// Base license validator.
	/// </summary>
	public abstract class AbstractLicenseValidator
	{
		/// <summary>
		/// License validator logger
		/// </summary>
		protected readonly Logger Logger = LogManager.GetCurrentClassLogger();

		private bool licenseInfoLogged;

		/// <summary>
		/// Standard Time servers
		/// </summary>
		protected readonly string[] TimeServers = new[]
		{
			"time.nist.gov",
			"time-nw.nist.gov",
			"time-a.nist.gov",
			"time-b.nist.gov",
			"time-a.timefreq.bldrdoc.gov",
			"time-b.timefreq.bldrdoc.gov",
			"time-c.timefreq.bldrdoc.gov",
			"utcnist.colorado.edu",
			"nist1.datum.com",
			"nist1.dc.certifiedtime.com",
			"nist1.nyc.certifiedtime.com",
			"nist1.sjc.certifiedtime.com"
		};

		private readonly string licenseServerUrl;
		private readonly Guid clientId;
		private readonly string publicKey;
		private readonly Timer nextLeaseTimer;
		private bool disableFutureChecks;
		private bool currentlyValidatingLicense;
		private readonly DiscoveryHost discoveryHost;
		private DiscoveryClient discoveryClient;
		private readonly Guid senderId = Guid.NewGuid();

		/// <summary>
		/// Fired when license data is invalidated
		/// </summary>
		public event Action<InvalidationType> LicenseInvalidated;

		/// <summary>
		/// Gets the expiration date of the license
		/// </summary>
		public DateTime ExpirationDate { get; private set; }

		/// <summary>
		/// How to behave when using the same license multiple times
		/// </summary>
		public MultipleLicenseUsage MultipleLicenseUsageBehavior { get; set; }

		/// <summary>
		/// Options for detecting multiple licenses
		/// </summary>
		public enum MultipleLicenseUsage
		{
			/// <summary>
			/// Deny if multiple licenses are used
			/// </summary>
			Deny,
			/// <summary>
			/// Only allow if it is running for the same user
			/// </summary>
			AllowForSameUser,
			/// <summary>
			/// Allow multiple copies of the same license to exist
			/// Usually valid for OEM scenarios
			/// </summary>
			AllowSameLicense
		}

		/// <summary>
		/// Gets or Sets the endpoint address of the subscription service
		/// </summary>
		public string SubscriptionEndpoint { get; set; }

		/// <summary>
		/// Gets the Type of the license
		/// </summary>
		public LicenseType LicenseType { get; private set; }

		/// <summary>
		/// Gets the Id of the license holder
		/// </summary>
		public Guid UserId { get; private set; }

		/// <summary>
		/// Gets the name of the license holder
		/// </summary>
		public string Name { get; private set; }

		/// <summary>
		/// Gets or Sets Floating license support
		/// </summary>
		public bool DisableFloatingLicenses { get; set; }

		/// <summary>
		/// Gets extra license information
		/// </summary>
		public IDictionary<string, string> LicenseAttributes { get; private set; }

		/// <summary>
		/// Gets or Sets the license content
		/// </summary>
		protected abstract string License { get; set; }

		private void LeaseLicenseAgain(object state)
		{
			if (License == null)
				return;

			var client = discoveryClient;
			if (client != null)
				client.PublishMyPresence();

			try
			{
				if (IsLicenseValid())
					return;
			}
			catch (RhinoLicensingException)
			{
				try
				{
					RaiseLicenseInvalidated();
				}
				catch (InvalidOperationException)
				{
					/* continue to RaiseLicenseInvalidated */
				}
				return;
			}

			RaiseLicenseInvalidated();
		}

		private void RaiseLicenseInvalidated()
		{
			var licenseInvalidated = LicenseInvalidated;
			if (licenseInvalidated == null)
				throw new InvalidOperationException("License was invalidated, but there is no one subscribe to the LicenseInvalidated event");
			licenseInvalidated(LicenseType == LicenseType.Floating
			                   	? InvalidationType.CannotGetNewLicense
			                   	: InvalidationType.TimeExpired);
		}

		/// <summary>
		/// Creates a license validator with specified public key.
		/// </summary>
		/// <param name="publicKey">public key</param>
		protected AbstractLicenseValidator(string publicKey)
		{
			LeaseTimeout = TimeSpan.FromMinutes(5);
			discoveryHost = new DiscoveryHost();
			LicenseAttributes = new Dictionary<string, string>();
			nextLeaseTimer = new Timer(LeaseLicenseAgain);
			this.publicKey = publicKey;
			discoveryHost.ClientDiscovered += DiscoveryHostOnClientDiscovered;
		}

		private void DiscoveryHostOnClientDiscovered(object sender, DiscoveryHost.ClientDiscoveredEventArgs clientDiscoveredEventArgs)
		{
			if (senderId == clientDiscoveredEventArgs.SenderId) // we got our own notification, ignore it
				return;
			if (UserId != clientDiscoveredEventArgs.UserId) // another license, we don't care
				return;

			// same user id, different senders
			switch (MultipleLicenseUsageBehavior)
			{
				case MultipleLicenseUsage.AllowSameLicense:
					return;
				case MultipleLicenseUsage.AllowForSameUser:
					if (Environment.UserName == clientDiscoveredEventArgs.UserName)
						return;
					break;
				case MultipleLicenseUsage.Deny:
					if (Environment.UserName == clientDiscoveredEventArgs.UserName &&
						Environment.MachineName == clientDiscoveredEventArgs.MachineName)
						return;
					break;
				default:
					throw new ArgumentOutOfRangeException("invalid MultipleLicenseUsageBehavior: " + MultipleLicenseUsageBehavior);
			}
			var client = discoveryClient;
			if (client != null)
			{
				client.PublishMyPresence();
			}
			RaiseLicenseInvalidated();
			var onMultipleLicensesWereDiscovered = MultipleLicensesWereDiscovered;
			if (onMultipleLicensesWereDiscovered != null)
			{
				onMultipleLicensesWereDiscovered(this, clientDiscoveredEventArgs);
			}
			else
			{
				throw new InvalidOperationException("Multiple licenses were discovered, but no one is handling the MultipleLicensesWereDiscovered event");
			}
		}

		public event EventHandler<DiscoveryHost.ClientDiscoveredEventArgs> MultipleLicensesWereDiscovered;

		/// <summary>
		/// Creates a license validator using the client information
		/// and a service endpoint address to validate the license.
		/// </summary>
		protected AbstractLicenseValidator(string publicKey, string licenseServerUrl, Guid clientId)
			: this(publicKey)
		{
			this.licenseServerUrl = licenseServerUrl;
			this.clientId = clientId;
		}

		/// <summary>
		/// Validates loaded license
		/// </summary>
		public virtual void AssertValidLicense()
		{
			AssertValidLicense(() => { });
		}

		/// <summary>
		/// Validates loaded license
		/// </summary>
		public virtual void AssertValidLicense(Action onValidLicense)
		{
			LicenseAttributes.Clear();
			if (IsLicenseValid())
			{
				onValidLicense();

				if (MultipleLicenseUsageBehavior == MultipleLicenseUsage.AllowSameLicense)
					return;

				try
				{
					discoveryHost.Start();
				}
				catch (Exception e)
				{
					// we explicitly don't want bad things to happen if we can't do that
					Logger.ErrorException("Could not setup node discovery", e);
				}

				discoveryClient = new DiscoveryClient(senderId, UserId, Environment.MachineName, Environment.UserName);
				discoveryClient.PublishMyPresence();
				return;
			}

			Logger.Warn("Could not validate existing license\r\n{0}", License);
			throw new LicenseNotFoundException("Could not find a valid license.");
		}

		private bool IsLicenseValid()
		{
			try
			{
				if (TryLoadingLicenseValuesFromValidatedXml() == false)
				{
					Logger.Warn("Failed validating license:\r\n{0}", License);
					return false;
				}
				if (licenseInfoLogged == false)
				{
					Logger.Info("License expiration date is {0}", ExpirationDate);
					licenseInfoLogged = true;
				}

				bool result;
				if (LicenseType == LicenseType.Subscription)
					result = ValidateLicense();
				else
				{
					result = DateTime.UtcNow < ExpirationDate;
					if (result)
						result = ValidateLicense();
				}

				if (result)
					ValidateUsingNetworkTime();
				else
					throw new LicenseExpiredException("Expiration Date : " + ExpirationDate);
				return true;
			}
			catch (RhinoLicensingException)
			{
				throw;
			}
			catch (Exception)
			{
				return false;
			}
		}

		private bool ValidateLicense()
		{
			if ((ExpirationDate - DateTime.UtcNow).TotalDays > 4)
				return true;

			if (currentlyValidatingLicense)
				return DateTime.UtcNow < ExpirationDate;

			if (SubscriptionEndpoint == null)
				throw new InvalidOperationException("Subscription endpoints are not supported for this license validator");

			try
			{
				TryGettingNewLeaseSubscription();
			}
			catch (Exception e)
			{
				Logger.ErrorException("Could not re-lease subscription license", e);
			}

			return ValidateWithoutUsingSubscriptionLeasing();
		}

		private bool ValidateWithoutUsingSubscriptionLeasing()
		{
			currentlyValidatingLicense = true;
			try
			{
				return IsLicenseValid();
			}
			finally
			{
				currentlyValidatingLicense = false;
			}
		}

		private void TryGettingNewLeaseSubscription()
		{
			var service = ChannelFactory<ISubscriptionLicensingService>.CreateChannel(new BasicHttpBinding(), new EndpointAddress(SubscriptionEndpoint));
			try
			{
				var newLicense = service.LeaseLicense(License);
				TryOverwritingWithNewLicense(newLicense);
			}
			catch (FaultException ex)
			{
				var message = ex.Message;
				if (message.StartsWith("The order has been cancelled"))
					throw new LicenseExpiredException(message);
				if (message.StartsWith("Invalid license"))
				{
					// Ignore.	
				}
			}
			catch (Exception e)
			{
				Logger.Error("Could not re-lease subscription license", e);
			}
			finally
			{
				var communicationObject = service as ICommunicationObject;
				if (communicationObject != null)
				{
					try
					{
						communicationObject.Close(TimeSpan.FromMilliseconds(200));
					}
					catch
					{
						communicationObject.Abort();
					}
				}
			}
		}

		/// <summary>
		/// Loads the license file.
		/// </summary>
		/// <param name="newLicense"></param>
		/// <returns></returns>
		protected bool TryOverwritingWithNewLicense(string newLicense)
		{
			if (string.IsNullOrEmpty(newLicense))
				return false;
			try
			{
				var xmlDocument = new XmlDocument();
				xmlDocument.LoadXml(newLicense);
			}
			catch (Exception e)
			{
				Logger.ErrorException("New license is not valid XML\r\n" + newLicense, e);
				return false;
			}
			License = newLicense;
			return true;
		}

		private void ValidateUsingNetworkTime()
		{
			if (!NetworkInterface.GetIsNetworkAvailable())
				return;

			var sntp = new SntpClient(TimeServers);
			sntp.BeginGetDate(time =>
			{
				if (time > ExpirationDate)
					RaiseLicenseInvalidated();
			}
			                  , () =>
			                  {
			                  	/* ignored */
			                  });
		}

		/// <summary>
		/// Removes existing license from the machine.
		/// </summary>
		public virtual void RemoveExistingLicense()
		{
			discoveryHost.Stop();
		}

		/// <summary>
		/// Loads license data from validated license file.
		/// </summary>
		/// <returns></returns>
		public bool TryLoadingLicenseValuesFromValidatedXml()
		{
			try
			{
				var doc = new XmlDocument();
				try
				{
					doc.LoadXml(License);
				}
				catch (Exception e)
				{
					throw new CorruptLicenseFileException("Could not understand the license, it isn't a valid XML file", e);
				}

				if (TryGetValidDocument(publicKey, doc) == false)
				{
					Logger.Warn("Could not validate xml signature of:\r\n{0}", License);
					return false;
				}

				if (doc.FirstChild == null)
				{
					Logger.Warn("Could not find first child of:\r\n{0}", License);
					return false;
				}

				if (doc.SelectSingleNode("/floating-license") != null)
				{
					var node = doc.SelectSingleNode("/floating-license/license-server-public-key/text()");
					if (node == null)
					{
						Logger.Warn("Invalid license, floating license without license server public key:\r\n{0}", License);
						throw new InvalidOperationException("Invalid license file format, floating license without license server public key");
					}
					return ValidateFloatingLicense(node.InnerText);
				}

				var result = ValidateXmlDocumentLicense(doc);
				if (result && disableFutureChecks == false)
				{
					nextLeaseTimer.Change(LeaseTimeout, LeaseTimeout);
				}
				return result;
			}
			catch (RhinoLicensingException)
			{
				throw;
			}
			catch (Exception e)
			{
				Logger.ErrorException("Could not validate license", e);
				return false;
			}
		}

		public TimeSpan LeaseTimeout { get; set; }

		private bool ValidateFloatingLicense(string publicKeyOfFloatingLicense)
		{
			if (DisableFloatingLicenses)
			{
				Logger.Warn("Floating licenses have been disabled");
				return false;
			}
			if (licenseServerUrl == null)
			{
				Logger.Warn("Could not find license server url");
				throw new InvalidOperationException("Floating license encountered, but licenseServerUrl was not set");
			}

			var success = false;
			var licensingService = ChannelFactory<ILicensingService>.CreateChannel(new WSHttpBinding(), new EndpointAddress(licenseServerUrl));
			try
			{
				var leasedLicense = licensingService.LeaseLicense(
					Environment.MachineName,
					Environment.UserName,
					clientId);
				((ICommunicationObject) licensingService).Close();
				success = true;
				if (leasedLicense == null)
				{
					Logger.Warn("Null response from license server: {0}", licenseServerUrl);
					throw new FloatingLicenseNotAvialableException();
				}

				var doc = new XmlDocument();
				doc.LoadXml(leasedLicense);

				if (TryGetValidDocument(publicKeyOfFloatingLicense, doc) == false)
				{
					Logger.Warn("Could not get valid license from floating license server {0}", licenseServerUrl);
					throw new FloatingLicenseNotAvialableException();
				}

				var validLicense = ValidateXmlDocumentLicense(doc);
				if (validLicense)
				{
					//setup next lease
					var time = (ExpirationDate.AddMinutes(-5) - DateTime.UtcNow);
					Logger.Debug("Will lease license again at {0}", time);
					if (disableFutureChecks == false)
						nextLeaseTimer.Change(time, time);
				}
				return validLicense;
			}
			finally
			{
				if (success == false)
					((ICommunicationObject) licensingService).Abort();
			}
		}

		internal bool ValidateXmlDocumentLicense(XmlDocument doc)
		{
			XmlNode id = doc.SelectSingleNode("/license/@id");
			if (id == null)
			{
				Logger.Warn("Could not find id attribute in license:\r\n{0}", License);
				return false;
			}

			UserId = new Guid(id.Value);

			XmlNode date = doc.SelectSingleNode("/license/@expiration");
			if (date == null)
			{
				Logger.Warn("Could not find expiration in license:\r\n{0}", License);
				return false;
			}

			ExpirationDate = DateTime.ParseExact(date.Value, "yyyy-MM-ddTHH:mm:ss.fffffff", CultureInfo.InvariantCulture);

			XmlNode licenseType = doc.SelectSingleNode("/license/@type");
			if (licenseType == null)
			{
				Logger.Warn("Could not find license type in {0}", licenseType);
				return false;
			}

			LicenseType = (LicenseType) Enum.Parse(typeof (LicenseType), licenseType.Value);

			XmlNode name = doc.SelectSingleNode("/license/name/text()");
			if (name == null)
			{
				Logger.Warn("Could not find licensee's name in license:\r\n{0}", License);
				return false;
			}

			Name = name.Value;

			var license = doc.SelectSingleNode("/license");
			foreach (XmlAttribute attrib in license.Attributes)
			{
				if (attrib.Name == "type" || attrib.Name == "expiration" || attrib.Name == "id")
					continue;

				LicenseAttributes[attrib.Name] = attrib.Value;
			}

			return true;
		}

		private bool TryGetValidDocument(string licensePublicKey, XmlDocument doc)
		{
			var rsa = new RSACryptoServiceProvider();
			rsa.FromXmlString(licensePublicKey);

			var nsMgr = new XmlNamespaceManager(doc.NameTable);
			nsMgr.AddNamespace("sig", "http://www.w3.org/2000/09/xmldsig#");

			var signedXml = new SignedXml(doc);
			var sig = (XmlElement) doc.SelectSingleNode("//sig:Signature", nsMgr);
			if (sig == null)
			{
				Logger.Warn("Could not find this signature node on license:\r\n{0}", License);
				return false;
			}
			signedXml.LoadXml(sig);

			return signedXml.CheckSignature(rsa);
		}

		/// <summary>
		/// Disables further license checks for the session.
		/// </summary>
		public void DisableFutureChecks()
		{
			disableFutureChecks = true;
			nextLeaseTimer.Dispose();
		}
	}
}