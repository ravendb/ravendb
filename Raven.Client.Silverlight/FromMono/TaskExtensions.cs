//
// TaskExtensions.cs
//
// Author:
//       Jérémie "Garuma" Laval <jeremie.laval@gmail.com>
//
// Copyright (c) 2010 Jérémie "Garuma" Laval
//
// Permission is hereby granted, free of charge, to any person obtaining
// a copy of this software and associated documentation files (the
// "Software"), to deal in the Software without restriction, including
// without limitation the rights to use, copy, modify, merge, publish,
// distribute, sublicense, and/or sell copies of the Software, and to
// permit persons to whom the Software is furnished to do so, subject to
// the following conditions:
// 
// The above copyright notice and this permission notice shall be
// included in all copies or substantial portions of the Software.
// 
// THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND,
// EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF
// MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND
// NONINFRINGEMENT. IN NO EVENT SHALL THE AUTHORS OR COPYRIGHT HOLDERS BE
// LIABLE FOR ANY CLAIM, DAMAGES OR OTHER LIABILITY, WHETHER IN AN ACTION
// OF CONTRACT, TORT OR OTHERWISE, ARISING FROM, OUT OF OR IN CONNECTION
// WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE SOFTWARE.
//


using System;
using System.Threading.Tasks;

namespace System.Threading.Tasks 
{
	public static class TaskExtensions
	{
		public static Task<TResult> Unwrap<TResult> (this Task<Task<TResult>> outer)
		{
			if (outer == null)
				throw new ArgumentNullException ("outer");

			TaskCompletionSource<TResult> src = new TaskCompletionSource<TResult> ();

			outer.ContinueWith (t1 => CopyCat (t1, src, () => t1.Result.ContinueWith (t2 => CopyCat (t2, src, () => src.TrySetResult (t2.Result)))));

			return src.Task;
		}

		public static Task Unwrap (this Task<Task> outer)
		{
			if (outer == null)
				throw new ArgumentNullException ("outer");

			TaskCompletionSource<object> src = new TaskCompletionSource<object> ();

			outer.ContinueWith (t1 => CopyCat (t1, src, () => t1.Result.ContinueWith (t2 => CopyCat (t2, src, () => src.TrySetResult (null)))));

			return src.Task;
		}

		static void CopyCat<TResult> (Task source,
		                              TaskCompletionSource<TResult> dest,
		                              Action normalAction)
		{
			if (source.IsFaulted)
				dest.TrySetException (source.Exception.InnerExceptions);
			else
				normalAction ();
		}
	}
}
