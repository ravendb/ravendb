//-----------------------------------------------------------------------
// <copyright file="IStack.cs" company="Hibernating Rhinos LTD">
//     Copyright (c) Hibernating Rhinos LTD. All rights reserved.
// </copyright>
//-----------------------------------------------------------------------
using System.Collections.Generic;

namespace Raven.Munin.Tree
{
	public interface IStack<T> : IEnumerable<T>
	{
		IStack<T> Pop();
		IStack<T> Push(T element);
		T Peek();
		bool IsEmpty { get; }
	}
}