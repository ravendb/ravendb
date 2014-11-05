using System;
using System.Text.RegularExpressions;

namespace Raven.Database.Server.RavenFS.Synchronization.Rdc.Wrapper
{
	public class SignatureInfo
	{
		private static readonly Regex SigFileNamePattern = new Regex(@"^(.*?)\.([0-9])\.sig$");

		public SignatureInfo(int level, string fileName)
		{
			FileName = fileName;
			Level = level;
		}

		public string FileName { get; private set; }

		public int Level { get; private set; }

		public string Name
		{
			get { return FileName + "." + Level + ".sig"; }
		}

		public long Length { get; set; }

		public static SignatureInfo Parse(string sigName)
		{
			var extracted = ExtractFileNameAndLevel(sigName);
			return new SignatureInfo(extracted.Item2, extracted.Item1);
		}

		private static Tuple<string, int> ExtractFileNameAndLevel(string sigName)
		{
			var matcher = SigFileNamePattern.Match(sigName);
			if (matcher.Success)
			{
				return new Tuple<string, int>(matcher.Groups[1].Value, int.Parse(matcher.Groups[2].Value));
			}
			throw new FormatException("SigName: " + sigName + " is not valid");
		}
	}
}