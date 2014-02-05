//-----------------------------------------------------------------------
// <copyright file="Stack.cs" company="Hibernating Rhinos LTD">
//     Copyright (c) Hibernating Rhinos LTD. All rights reserved.
// </copyright>
//-----------------------------------------------------------------------
using System.Collections;
using System.Collections.Generic;

namespace Raven.Munin.Tree
{
	public sealed class Stack<T> : IStack<T>
	{
		private static readonly EmptyStack<T> empty = new EmptyStack<T>();
		public static IStack<T> Empty { get { return empty; } }
		private readonly T head;
		private readonly IStack<T> tail;

		internal Stack(T head, IStack<T> tail)
		{
			this.head = head;
			this.tail = tail;
		}
		public bool IsEmpty { get { return false; } }
		public T Peek() { return head; }
		public IStack<T> Pop() { return tail; }
		public IStack<T> Push(T element) { return new Stack<T>(element, this); }
		public IEnumerator<T> GetEnumerator()
		{
			for (IStack<T> stack = this; !stack.IsEmpty; stack = stack.Pop())
				yield return stack.Peek();
		}
		IEnumerator IEnumerable.GetEnumerator() { return this.GetEnumerator(); }
	}
}