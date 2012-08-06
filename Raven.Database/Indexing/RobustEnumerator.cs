//-----------------------------------------------------------------------
// <copyright file="RobustEnumerator.cs" company="Hibernating Rhinos LTD">
//     Copyright (c) Hibernating Rhinos LTD. All rights reserved.
// </copyright>
//-----------------------------------------------------------------------
using System;
using System.Collections;
using System.Collections.Generic;
using Raven.Database.Linq;
using Raven.Database.Storage;
using System.Linq;

namespace Raven.Database.Indexing
{
	public class RobustEnumerator
	{
		public Action BeforeMoveNext = delegate { };
		public Action CancelMoveNext = delegate { };
		public Action<Exception, object> OnError = delegate { };
		private readonly WorkContext context;
		private readonly int numberOfConsecutiveErrors;

		public RobustEnumerator(WorkContext context, int numberOfConsecutiveErrors)
		{
			this.context = context;
			this.numberOfConsecutiveErrors = numberOfConsecutiveErrors;
		}

		public IEnumerable<object> RobustEnumeration(IEnumerable<object> input, IndexingFunc func)
		{
			return RobustEnumeration(input, new[] { func, });
		}

		public IEnumerable<object> RobustEnumeration(IEnumerable<object> input, IEnumerable<IndexingFunc> funcs)
		{
			List<object> onlyIterateOverEnumableOnce;
			try
			{
				onlyIterateOverEnumableOnce = input.ToList();
			}
			catch (Exception e)
			{
				OnError(e, null);
				yield break;
			}

			foreach (var func in funcs)
			{
				using (var wrapped = new StatefulEnumerableWrapper<dynamic>(onlyIterateOverEnumableOnce.GetEnumerator()))
				{
					IEnumerator<dynamic> en;
					using (en = func(wrapped).GetEnumerator())
					{
						int maxNumberOfConsecutiveErrors = numberOfConsecutiveErrors;
						do
						{
							context.CancellationToken.ThrowIfCancellationRequested();
							var moveSuccessful = MoveNext(en, wrapped);
							if (moveSuccessful == false)
								break;
							if (moveSuccessful == true)
							{
								maxNumberOfConsecutiveErrors = numberOfConsecutiveErrors;
								yield return en.Current;
							}
							else
							{
								// we explictly do not dispose the enumerator, since that would not allow us 
								// to continue on with the next item in the list.
								// Not actually a problem, because we are iterating only over in memory data
								// en.Dispose();

								en = func(wrapped).GetEnumerator();
								maxNumberOfConsecutiveErrors--;
							}
						} while (maxNumberOfConsecutiveErrors > 0);
					}
				}
			}
		}

		private bool? MoveNext(IEnumerator en, StatefulEnumerableWrapper<object> innerEnumerator)
		{
			try
			{
				BeforeMoveNext();
				var moveNext = en.MoveNext();
				if (moveNext == false)
				{
					CancelMoveNext();
				}
				return moveNext;
			}
			catch (Exception e)
			{
				OnError(e, innerEnumerator.Current);
			}
			return null;
		}
	}
}