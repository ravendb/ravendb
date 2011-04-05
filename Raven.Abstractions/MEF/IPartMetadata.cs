using System;
using System.ComponentModel;

namespace Raven.Abstractions.MEF
{
	public interface IPartMetadata
	{
		[DefaultValue(0)]
		int Order { get; }
	}

	public class PartMetadata : IPartMetadata
	{
		public int Order { get; set; }
	}
}