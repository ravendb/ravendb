using Raven.Abstractions;

namespace Rhino.Licensing
{
	using System;
	using System.Collections.Generic;
	using System.Diagnostics;
	using System.IO;
	using System.ServiceModel;
	using System.Linq;

	/// <summary>
	/// Licensing server implementation.
	/// Because we use this service behavior, we don't have to worry 
	/// about multi threading issues. it is not something that we 
	/// expect to have to deal with huge load, anyway.
	/// </summary>
	[ServiceBehavior(InstanceContextMode = InstanceContextMode.Single, ConcurrencyMode = ConcurrencyMode.Single)]
	public class LicensingService : ILicensingService
	{
		private readonly List<LicenseValidator> availableLicenses = new List<LicenseValidator>();
		private readonly Dictionary<string, KeyValuePair<DateTime, LicenseValidator>> leasedLicenses = new Dictionary<string, KeyValuePair<DateTime, LicenseValidator>>();
		private readonly string state;

		/// <summary>
		/// Creates a new instance of <seealso cref="LicensingService"/>.
		/// </summary>
		public LicensingService()
		{
			if (SoftwarePublicKey == null)
				throw new InvalidOperationException("SoftwarePublicKey must be set before starting the service");

			if (LicenseServerPrivateKey == null)
				throw new InvalidOperationException("LicenseServerPrivateKey must be set before starting the service");

			var licensesDirectory = Path.Combine(AppDomain.CurrentDomain.BaseDirectory, "Licenses");
			state = Path.Combine(AppDomain.CurrentDomain.BaseDirectory, "licenseServer.state");

			EnsureLicenseDirectoryExists(licensesDirectory);

			ReadAvailableLicenses(licensesDirectory);

			ReadInitialState();
		}

		/// <summary>
		/// Gets or Sets the public key of the product
		/// </summary>
		public static string SoftwarePublicKey
		{
			get;
			set;
		}

		/// <summary>
		/// Gets or Sets the private key of the license server
		/// </summary>
		public static string LicenseServerPrivateKey
		{
			get;
			set;
		}

		private static void EnsureLicenseDirectoryExists(string licensesDirectory)
		{
			if (Directory.Exists(licensesDirectory) == false)
			{
				try
				{
					Directory.CreateDirectory(licensesDirectory);
				}
				catch (Exception e)
				{
					throw new DirectoryNotFoundException("Could not find licenses directory: " + licensesDirectory, e);
				}
			}
		}

		private void ReadAvailableLicenses(string licensesDirectory)
		{
			foreach (var license in Directory.GetFiles(licensesDirectory, "*.xml"))
			{
				var set = new HashSet<Guid>();
				var validator = new LicenseValidator(SoftwarePublicKey, license)
				{
					DisableFloatingLicenses = true
				};
				try
				{
					validator.AssertValidLicense();
					Debug.WriteLine("Found license for " + validator.Name + " of type: " + validator.LicenseType);
					if (validator.LicenseType == LicenseType.Standard &&
						// this prevent a simple cheating of simply copying the same
						// license file several times
						set.Add(validator.UserId))
					{
						availableLicenses.Add(validator);
						Debug.WriteLine("Accepting license for: " + validator.Name + " " + validator.UserId);
					}
				}
				catch (Exception)
				{
					continue;
				}
			}
		}

		private void ReadInitialState()
		{
			try
			{
				using (var file = new FileStream(state, FileMode.OpenOrCreate, FileAccess.ReadWrite))
				{
					ReadState(file);
				}
			}
			catch (AccessViolationException e)
			{
				throw new AccessViolationException("Could not open file '" + state + "' for read/write, please grant read/write access to the file.", e);
			}
			catch (Exception e)
			{
				throw new InvalidOperationException("Could not understand file '" + state + "'.", e);
			}
		}

		private void ReadState(Stream stream)
		{
			try
			{
				using (var binaryReader = new BinaryReader(stream))
				{
					while (true)
					{
						var identifier = binaryReader.ReadString();
						var time = DateTime.FromBinary(binaryReader.ReadInt64());
						var userId = new Guid(binaryReader.ReadBytes(16));

						var licenseValidator = availableLicenses.FirstOrDefault(x => x.UserId == userId);
						if (licenseValidator == null)
							continue;

						leasedLicenses[identifier] = new KeyValuePair<DateTime, LicenseValidator>(time, licenseValidator);
						availableLicenses.Remove(licenseValidator);
					}
				}
			}
			catch (EndOfStreamException)
			{
			}
		}

		private void WriteState(Stream stream)
		{
			using (var binaryWriter = new BinaryWriter(stream))
			{
				foreach (var pair in leasedLicenses)
				{
					binaryWriter.Write(pair.Key);
					binaryWriter.Write(pair.Value.Key.ToBinary());
					binaryWriter.Write(pair.Value.Value.UserId.ToByteArray());
				}
				binaryWriter.Flush();
				stream.Flush();
			}
		}


		/// <summary>
		/// Leases a new license to the client machine.
		/// </summary>
		/// <param name="machine">client machine name</param>
		/// <param name="user">user name</param>
		/// <param name="id">Id of the license holder</param>
		/// <returns></returns>
		public string LeaseLicense(
			string machine,
			string user,
			Guid id)
		{
			KeyValuePair<DateTime, LicenseValidator> value;
			var identifier = machine + @"\" + user + ": " + id;
			if (leasedLicenses.TryGetValue(identifier, out value))
			{
				Debug.WriteLine(id + " is already leased, so extending lease");
				var licenseValidator = value.Value;
				return GenerateLicenseAndRenewLease(identifier, id, licenseValidator, value.Value.LicenseAttributes);
			}
			if (availableLicenses.Count > 0)
			{
				var availableLicense = availableLicenses[availableLicenses.Count - 1];
				availableLicenses.RemoveAt(availableLicenses.Count - 1);
				Debug.WriteLine("Found available license to give, leasing it");
				return GenerateLicenseAndRenewLease(identifier, id, availableLicense, availableLicense.LicenseAttributes);
			}
			foreach (var kvp in leasedLicenses)
			{
				if ((SystemTime.UtcNow - kvp.Value.Key).TotalMinutes < 45)
					continue;
				leasedLicenses.Remove(kvp.Key);
				Debug.WriteLine("Found expired leased license, leasing it");
				return GenerateLicenseAndRenewLease(identifier, id, kvp.Value.Value, kvp.Value.Value.LicenseAttributes);
			}
			Debug.WriteLine("Could not find license to lease");
			return null;
		}

		private string GenerateLicenseAndRenewLease(string identifier, Guid id, LicenseValidator licenseValidator, IDictionary<string, string> attributes)
		{
			leasedLicenses[identifier] = new KeyValuePair<DateTime, LicenseValidator>(SystemTime.UtcNow.AddMinutes(30), licenseValidator);
			using (var file = new FileStream(state, FileMode.Create, FileAccess.ReadWrite))
			{
				WriteState(file);
			}
			return GenerateLicense(id, licenseValidator, attributes);
		}

		private static string GenerateLicense(Guid id, LicenseValidator validator, IDictionary<string, string> attributes)
		{
			var generator = new LicenseGenerator(LicenseServerPrivateKey);
			return generator.Generate(validator.Name, id, SystemTime.UtcNow.AddMinutes(45), attributes, LicenseType.Floating);
		}
	}
}