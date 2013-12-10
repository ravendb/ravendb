using System;
using System.Collections.Specialized;
using System.IO;
using System.Web;
using Raven.Database.Extensions;
using Raven.Database.Util;

namespace Raven.Database.Server.RavenFS.Config
{
	public class InMemoryConfigurationOld
	{
		private string dataDirectory;
		private string indexStoragePath;
		private string serverUrl;
		private string virtualDirectory;

		public InMemoryConfigurationOld()
		{
			Settings = new NameValueCollection(StringComparer.InvariantCultureIgnoreCase);
		}

		public NameValueCollection Settings { get; set; }

		public string DataDirectory
		{
			get { return dataDirectory; }
			set { dataDirectory = value == null ? null : value.ToFullPath(); }
		}

		public string IndexStoragePath
		{
			get
			{
				if (string.IsNullOrEmpty(indexStoragePath))
					return Path.Combine(DataDirectory, "Index.ravenfs");

				return indexStoragePath;
			}
			set { indexStoragePath = value.ToFullPath(); }
		}

		public string ServerUrl
		{
			get
			{
				if (!string.IsNullOrEmpty(serverUrl) && ServerUrlUtil.TryReadPreviouslySavedServerUrl(out serverUrl))
				{
					return serverUrl;
				}

				HttpRequest httpRequest = null;
				try
				{
					if (HttpContext.Current != null)
						httpRequest = HttpContext.Current.Request;
				}
				catch (Exception)
				{
					// the issue is probably Request is not available in this context
					// we can safely ignore this, at any rate
				}
				if (httpRequest != null) // running in IIS, let us figure out how
				{
					var url = httpRequest.Url;
					return new UriBuilder(url)
					{
						Path = httpRequest.ApplicationPath,
						Query = ""
					}.Uri.ToString();
				}

				return new UriBuilder("http", (HostName ?? Environment.MachineName), Port, VirtualDirectory).Uri.ToString();
			}
		}

		public string VirtualDirectory
		{
			get { return virtualDirectory; }
			set
			{
				virtualDirectory = value;

				if (virtualDirectory.EndsWith("/"))
					virtualDirectory = virtualDirectory.Substring(0, virtualDirectory.Length - 1);
				if (virtualDirectory.StartsWith("/") == false)
					virtualDirectory = "/" + virtualDirectory;
			}
		}

		public string HostName { get; set; }

		public int Port { get; set; }

		public void Initialize()
		{
			// Data settings
			DataDirectory = Settings["Raven/DataDir"] ?? @"~\Data.ravenfs";

			if (string.IsNullOrEmpty(Settings["Raven/IndexStoragePath"]) == false)
			{
				IndexStoragePath = Settings["Raven/IndexStoragePath"];
			}

			// HTTP Settings
			HostName = Settings["Raven/HostName"];

			Port = PortUtil.GetPort(Settings["Raven/Port"]);

			SetVirtualDirectory();
		}

		private void SetVirtualDirectory()
		{
			var defaultVirtualDirectory = "/";
			try
			{
				if (HttpContext.Current != null)
					defaultVirtualDirectory = HttpContext.Current.Request.ApplicationPath;
			}
			catch (HttpException)
			{
			}

			VirtualDirectory = Settings["Raven/VirtualDirectory"] ?? defaultVirtualDirectory;
		}
	}
}