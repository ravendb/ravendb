namespace Raven.Studio.Plugins.Database
{
	using System;
	using System.ComponentModel.Composition;

	[MetadataAttribute]
	[AttributeUsage(AttributeTargets.Class, AllowMultiple = false)]
	public class ExportDatabaseScreenAttribute : ExportAttribute
	{
		public ExportDatabaseScreenAttribute(string displayName)
			: base("Raven.DatabaseExplorerItem")
		{
			DisplayName = displayName;
		}

		public string DisplayName { get; private set; }
		public int Index { get; set; }
	}
}