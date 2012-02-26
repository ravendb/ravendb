//-----------------------------------------------------------------------
// <copyright file="ExportSpec.cs" company="Hibernating Rhinos LTD">
//     Copyright (c) Hibernating Rhinos LTD. All rights reserved.
// </copyright>
//-----------------------------------------------------------------------
namespace Raven.Smuggler
{
	public class ExportSpec
	{
		public ExportSpec(string file, bool exportIndexesOnly, bool includeAttachments)
		{
			File = file;
			ExportIndexesOnly = exportIndexesOnly;
			IncludeAttachments = includeAttachments;
		}

		public string File { get; private set; }

		public bool ExportIndexesOnly { get; private set; }

		public bool IncludeAttachments { get; private set; }
	}
}