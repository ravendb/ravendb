using System;
using System.ComponentModel;

namespace Raven.Abstractions.MEF
{
	public interface IPartMetadata
	{
		[DefaultValue(0)]
		int Order { get; }
	}
}