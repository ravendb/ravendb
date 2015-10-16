// -----------------------------------------------------------------------
//  <copyright file="SmuggleType.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------
namespace Raven.Smuggler.Database
{
	public enum SmuggleType
	{
		None,

		Index,

		Document,

		Transformer,

		DocumentDeletion,

		Identity,

		Attachment,

		AttachmentDeletion
	}
}