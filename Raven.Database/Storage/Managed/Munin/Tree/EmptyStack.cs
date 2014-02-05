//-----------------------------------------------------------------------
// <copyright file="EmptyStack.cs" company="Hibernating Rhinos LTD">
//     Copyright (c) Hibernating Rhinos LTD. All rights reserved.
// </copyright>
//-----------------------------------------------------------------------
using System;
using System.Collections;
using System.Collections.Generic;

namespace Raven.Munin.Tree
{
	internal sealed class EmptyStack<T> : IStack<T>
	{
		public bool IsEmpty { get { return true; } }
		public IStack<T> Push(T element)
		{
			return new Stack<T>(element, this);
		}

		public T Peek() { throw new Exception("Empty stack"); }
		public IStack<T> Pop() { throw new Exception("Empty stack"); }
		public IEnumerator<T> GetEnumerator() { yield break; }
		IEnumerator IEnumerable.GetEnumerator() { return this.GetEnumerator(); }
	}
}