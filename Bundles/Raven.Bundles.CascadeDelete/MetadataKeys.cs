//-----------------------------------------------------------------------
// <copyright file="MetadataKeys.cs" company="Hibernating Rhinos LTD">
//     Copyright (c) Hibernating Rhinos LTD. All rights reserved.
// </copyright>
//-----------------------------------------------------------------------

namespace Raven.Bundles.CascadeDelete
{
	public static class MetadataKeys
	{
		public static readonly string DocumentsToCascadeDelete = "Raven-Cascade-Delete-Documents";
		public static readonly string AttachmentsToCascadeDelete = "Raven-Cascade-Delete-Attachments";
	}
}
