namespace Raven.Studio.Features.Database
{
	using System;
	using System.ComponentModel.Composition;
    using Caliburn.Micro;

    [InheritedExport]
	public interface IDatabaseScreenMenuItem : IScreen
	{
	}

	[MetadataAttribute]
	[AttributeUsage(AttributeTargets.Class, AllowMultiple = false)]
	public class ExportDatabaseScreenAttribute : ExportAttribute
	{
		public ExportDatabaseScreenAttribute(string displayName) : base(typeof(IDatabaseScreenMenuItem)) { DisplayName = displayName; }

		public string DisplayName { get; private set; }
		public int Index { get; set; }
	}
}