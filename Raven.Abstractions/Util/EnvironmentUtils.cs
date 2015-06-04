using System;

namespace Raven.Abstractions
{
	public class EnvironmentUtils
	{
		static readonly bool runningOnMono = Type.GetType("Mono.Runtime") != null;
		public static bool RunningOnMono
		{
			get { return runningOnMono; }
		}


		public static bool RunningOnPosix
		{
			get
			{
				switch (Environment.OSVersion.Platform)
				{
				case PlatformID.Win32S:
				case PlatformID.Win32Windows:
				case PlatformID.Win32NT:
				case PlatformID.WinCE:
				case PlatformID.Xbox:
					return false;
				case PlatformID.Unix:
				case PlatformID.MacOSX:
					return true;
				default:
					return false; // we'll try the windows version here
				}
			}
		}	}
}

