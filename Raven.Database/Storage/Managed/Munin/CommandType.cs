//-----------------------------------------------------------------------
// <copyright file="CommandType.cs" company="Hibernating Rhinos LTD">
//     Copyright (c) Hibernating Rhinos LTD. All rights reserved.
// </copyright>
//-----------------------------------------------------------------------
namespace Raven.Munin
{
	public enum CommandType : byte
	{
		Put = 1,
		Delete = 2,
		Skip = 9
	}
}