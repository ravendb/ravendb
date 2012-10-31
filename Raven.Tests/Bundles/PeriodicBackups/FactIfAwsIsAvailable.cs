using System;
using System.IO;
using System.Linq;
using System.Xml;
using Xunit;
using Xunit.Sdk;

namespace Raven.Tests.Bundles.PeriodicBackups
{
	[CLSCompliant(false)]
	public class FactIfAwsIsAvailable : FactAttribute
	{
		private static string AwsAccessKey, AwsSecretKey;

		public FactIfAwsIsAvailable()
		{
			if (!string.IsNullOrWhiteSpace(AwsSecretKey) && !string.IsNullOrWhiteSpace(AwsAccessKey))
				return;

			string fullPath = null;
			var curDir = Directory.GetCurrentDirectory();
			while (true)
			{
				var dir = Directory.GetParent(curDir);
				if (dir == null || dir.Equals(dir.Root)) break;
				curDir = dir.FullName;
				if (File.Exists(Path.Combine(curDir, "AwsCredentials.txt")))
				{
					fullPath = Path.Combine(curDir, "AwsCredentials.txt");
					break;
				}
			}

			if (fullPath == null)
				base.Skip = "Could not find AWS credentials";

			try
			{
				using (var fs = (File.OpenText(fullPath)))
				{
					AwsAccessKey = fs.ReadLine();
					AwsSecretKey = fs.ReadLine();
				}
			}
			catch
			{
				base.Skip = "Could not find AWS credentials";
			}
		}

		protected override System.Collections.Generic.IEnumerable<Xunit.Sdk.ITestCommand> EnumerateTestCommands(Xunit.Sdk.IMethodInfo method)
		{
			return base.EnumerateTestCommands(method)
				.Select(enumerateTestCommand =>
					new ActionTestCommandWrapper(enumerateTestCommand, o => ((PeriodicBackupTests)o).SetupAws(AwsAccessKey, AwsSecretKey)));
		}

		public class ActionTestCommandWrapper : ITestCommand
		{
			private readonly ITestCommand inner;
			private readonly Action<object> action;

			public ActionTestCommandWrapper(ITestCommand inner, Action<object> action)
			{
				this.inner = inner;
				this.action = action;
			}

			public MethodResult Execute(object testClass)
			{
				action(testClass);
				return inner.Execute(testClass);
			}

			public XmlNode ToStartXml()
			{
				return inner.ToStartXml();
			}

			public string DisplayName
			{
				get { return inner.DisplayName; }
			}

			public bool ShouldCreateInstance
			{
				get { return inner.ShouldCreateInstance; }
			}

			public int Timeout
			{
				get { return inner.Timeout; }
			}
		}
	}
}
