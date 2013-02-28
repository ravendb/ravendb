//-----------------------------------------------------------------------
// <copyright file="NonAdminHttp.cs" company="Hibernating Rhinos LTD">
//     Copyright (c) Hibernating Rhinos LTD. All rights reserved.
// </copyright>
//-----------------------------------------------------------------------
using System;
using System.Diagnostics;
using System.Net;
using System.Security.Cryptography.X509Certificates;
using System.Security.Principal;

namespace Raven.Database.Server
{
	public static class NonAdminHttp
	{
		public static void EnsureCanListenToWhenInNonAdminContext(int port, bool useSsl = false, string sslCertificatePath = null, string sslCertificatePassword = null)
		{
			int errorCode;
			int grantCode;
			int unregisterCode = 0;
			HttpListenerException listenerException;

			if (CanStartHttpListener(port, useSsl, out errorCode, out listenerException))
			{
				RebindCertificates(port, useSsl, sslCertificatePath, sslCertificatePassword);
				return;
			}

			switch (errorCode)
			{
				case 5:		// access denied
					grantCode = TryGrantingHttpPrivileges(port, useSsl);
					RebindCertificates(port, useSsl, sslCertificatePath, sslCertificatePassword);
					break;
				case 183:	// conflict
					unregisterCode = TryUnregisterHttpPort(port, useSsl);
					grantCode = TryGrantingHttpPrivileges(port, useSsl);
					RebindCertificates(port, useSsl, sslCertificatePath, sslCertificatePassword);
					break;
				default:
					throw new InvalidOperationException("Could not listen to port " + port, listenerException);
			}

			if (CanStartHttpListener(port, useSsl, out errorCode, out listenerException) == false)
				Console.WriteLine("Failed to grant rights for listening to http, exit codes: ({0} and {1})", grantCode, unregisterCode);
		}

		private static void RebindCertificates(int port, bool useSsl, string sslCertificatePath, string sslCertificatePassword)
		{
			if (string.IsNullOrEmpty(sslCertificatePath) && useSsl)
				throw new InvalidOperationException("Certificate path cannot be null when SSL is enabled.");

			X509Certificate2 certificate = null;

			if (!string.IsNullOrEmpty(sslCertificatePath))
			{
				certificate = !string.IsNullOrEmpty(sslCertificatePassword) ? new X509Certificate2(sslCertificatePath, sslCertificatePassword) : new X509Certificate2(sslCertificatePath);
			}
				
			UnbindCertificates(port, certificate != null ? certificate.Thumbprint : null);

			if (useSsl)
				BindCertificate(port, certificate != null ? certificate.Thumbprint : null);
		}

		private static void UnbindCertificates(int port, string certificateThumbprint)
		{
			string cmd;
			string args;

			GetArgsForHttpSslCertDeleteCmd(port, certificateThumbprint, out args, out cmd);

			RunAs(cmd, args, -188);
		}

		private static void BindCertificate(int port, string certificateThumbprint)
		{
			var applicationId = Guid.NewGuid();

			string cmd;
			string args;

			GetArgsForHttpSslCertAddCmd(port, certificateThumbprint, applicationId, out args, out cmd);

			RunAs(cmd, args, -177);
		}

		private static void GetArgsForHttpSslCertAddCmd(int port, string certificateThumbprint, Guid applicationId, out string args, out string cmd)
		{
			if (Environment.OSVersion.Version.Major > 5)
			{
				cmd = "netsh";
				args = string.Format(@"http add sslcert ipport=0.0.0.0:{0} certhash={1} appid={{{2}}}", port, certificateThumbprint, applicationId);
			}
			else
			{
				cmd = "httpcfg";
				args = string.Format(@"set ssl -i 0.0.0.0:{0} -h {1}", port, certificateThumbprint);
			}
		}

		private static void GetArgsForHttpSslCertDeleteCmd(int port, string certificateThumbprint, out string args, out string cmd)
		{
			if (Environment.OSVersion.Version.Major > 5)
			{
				cmd = "netsh";
				args = string.Format(@"http delete sslcert ipport=0.0.0.0:{0}", port);
			}
			else
			{
				cmd = "httpcfg";
				args = string.Format(@"delete ssl -i 0.0.0.0:{0} -h {1}", port, certificateThumbprint);
			}
		}

		private static void GetArgsForHttpAclAddCmd(int port, bool useSsl, out string args, out string cmd)
		{
			if (Environment.OSVersion.Version.Major > 5)
			{
				cmd = "netsh";
				args = string.Format(@"http add urlacl url={2}+:{0}/ user=""{1}""", port, WindowsIdentity.GetCurrent().Name, useSsl ? "https://" : "http://");
			}
			else
			{
				cmd = "httpcfg";
				args = string.Format(@"set urlacl /u {2}+:{0}/ /a D:(A;;GX;;;""{1}"")", port, WindowsIdentity.GetCurrent().User, useSsl ? "https://" : "http://");
			}
		}

		private static void GetArgsForHttpAclDeleteCmd(int port, bool useSsl, out string args, out string cmd)
		{
			if (Environment.OSVersion.Version.Major > 5)
			{
				cmd = "netsh";
				args = string.Format(@"http delete urlacl url={1}+:{0}/", port, useSsl ? "http://" : "https://");
			}
			else
			{
				cmd = "httpcfg";
				args = string.Format(@"delete urlacl /u {1}+:{0}/", port, useSsl ? "https://" : "http://");
			}
		}


		private static bool CanStartHttpListener(int port, bool useSsl, out int errorCode, out HttpListenerException listenerException)
		{
			errorCode = 0;
			listenerException = null;

			try
			{
				var httpListener = new HttpListener();
				httpListener.Prefixes.Add(string.Format("{0}+:{1}/", useSsl ? "https://" : "http://", port));
				httpListener.Start();
				httpListener.Stop();
				return true;
			}
			catch (HttpListenerException e)
			{
				errorCode = e.ErrorCode;
				listenerException = e;
			}

			return false;
		}

		private static int TryUnregisterHttpPort(int port, bool useSsl)
		{
			string args;
			string cmd;
			GetArgsForHttpAclDeleteCmd(port, useSsl, out args, out cmd);

			Console.WriteLine("Trying to revoke rights for http.sys");
			return RunAs(cmd, args, -155);
		}

		private static int TryGrantingHttpPrivileges(int port, bool useSsl)
		{
			string args;
			string cmd;
			GetArgsForHttpAclAddCmd(port, useSsl, out args, out cmd);

			Console.WriteLine("Trying to grant rights for http.sys");
			return RunAs(cmd, args, -144);
		}

		private static int RunAs(string cmd, string args, int errorCode)
		{
			try
			{
				Console.WriteLine("runas {0} {1}", cmd, args);
				var process = Process.Start(new ProcessStartInfo
				{
					Verb = "runas",
					Arguments = args,
					FileName = cmd,
				});
				process.WaitForExit();
				return process.ExitCode;
			}
			catch (Exception)
			{
				return errorCode;
			}
		}
	}
}
