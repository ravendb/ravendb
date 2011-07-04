using System;
using System.ComponentModel.Composition;

namespace Raven.Studio.Infrastructure.Navigation
{
	[MetadataAttribute]
	[AttributeUsage(AttributeTargets.Class, AllowMultiple = false)]
	public class NavigatorExportAttribute : ExportAttribute
	{
		public NavigatorExportAttribute(string url)
			: base(typeof(INavigator))
		{
			Url = url;
		}

		public string Url { get; private set; }
		public int Index { get; set; }
	}
}