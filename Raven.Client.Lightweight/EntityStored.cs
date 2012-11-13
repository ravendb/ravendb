//-----------------------------------------------------------------------
// <copyright file="EntityStored.cs" company="Hibernating Rhinos LTD">
//     Copyright (c) Hibernating Rhinos LTD. All rights reserved.
// </copyright>
//-----------------------------------------------------------------------
namespace Raven.Client
{
	/// <summary>
	/// Delegate definition when an entity is stored in the session
	/// </summary>
	public delegate void EntityStored(object entity);
}
