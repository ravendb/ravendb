using System;
using System.Runtime.InteropServices;

namespace Raven.Server
{

	/// <summary>
	/// Checks for redirected output before clearing
	/// </summary>
	/// <remarks>
	/// Original Source: http://stackoverflow.com/a/3453272/837001
	/// </remarks>
	public static class ConsoleEx
	{
		private static bool IsOutputRedirected
		{
			get
			{
				return FileType.Char != GetFileType(GetStdHandle(StdHandle.Stdout));
			}
		}
		private static bool IsInputRedirected
		{
			get
			{
				return FileType.Char != GetFileType(GetStdHandle(StdHandle.Stdin));
			}
		}
		private static bool IsErrorRedirected
		{
			get
			{
				return FileType.Char != GetFileType(GetStdHandle(StdHandle.Stderr));
			}
		}

		public static void ClearIfNotRedirected()
		{
			if (!IsOutputRedirected && !IsInputRedirected && !IsErrorRedirected)
				Console.Clear();
		}

		// P/Invoke:
		private enum FileType
		{
			Unknown,
			Disk,
			Char,
			Pipe
		};

		private enum StdHandle
		{
			Stdin = -10,
			Stdout = -11,
			Stderr = -12
		};

		[DllImport("kernel32.dll")]
		private static extern FileType GetFileType(IntPtr hdl);

		[DllImport("kernel32.dll")]
		private static extern IntPtr GetStdHandle(StdHandle std);

	}

}