using System;

namespace Raven.Abstractions.Database.Smuggler.Data
{
	[Flags]
	public enum ItemType
	{
		Documents = 0x1,
		Indexes = 0x2,

		Transformers = 0x8,

		RemoveAnalyzers = 0x8000,
	}
}