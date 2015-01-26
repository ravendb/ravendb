// -----------------------------------------------------------------------
//  <copyright file="StructReadResult.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------
namespace Voron
{
	public class StructReadResult<TStruct> where TStruct : struct 
	{
		public StructReadResult(TStruct value, ushort version)
		{
			Value = value;
			Version = version;
		}

		public TStruct Value;

		public ushort Version { get; private set; }
	}
}