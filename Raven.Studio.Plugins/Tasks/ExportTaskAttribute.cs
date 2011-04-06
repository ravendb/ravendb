namespace Raven.Studio.Plugins.Tasks
{
	using System;
	using System.ComponentModel.Composition;

	[MetadataAttribute]
	[AttributeUsage(AttributeTargets.Class, AllowMultiple = false)]
	public class ExportTaskAttribute : ExportAttribute
	{
		public ExportTaskAttribute(string displayName) : base("Raven.Task") { DisplayName = displayName; }

		public string DisplayName { get; private set; }
		public int Index { get; set; }
	}
}