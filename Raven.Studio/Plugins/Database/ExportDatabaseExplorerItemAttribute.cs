namespace Raven.Studio.Plugins.Database
{
	using System;
	using System.ComponentModel.Composition;

	[MetadataAttribute]
	[AttributeUsage(AttributeTargets.Class, AllowMultiple = false)]
	public class ExportDatabaseExplorerItemAttribute : ExportAttribute
	{
		public ExportDatabaseExplorerItemAttribute()
			: base("Raven.DatabaseExplorerItem")
		{
		}

		public string DisplayName { get; set; }
		public int Index { get; set; }
	}
}