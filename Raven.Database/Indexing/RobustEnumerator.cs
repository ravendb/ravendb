//-----------------------------------------------------------------------
// <copyright file="RobustEnumerator.cs" company="Hibernating Rhinos LTD">
//     Copyright (c) Hibernating Rhinos LTD. All rights reserved.
// </copyright>
//-----------------------------------------------------------------------
using System;
using System.Collections;
using System.Collections.Generic;
using System.Threading;
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
		private readonly CancellationToken cancellationToken;
		private readonly int numberOfConsecutiveErrors;

		public RobustEnumerator(CancellationToken cancellationToken, int numberOfConsecutiveErrors)
		{
			this.cancellationToken = cancellationToken;
			this.numberOfConsecutiveErrors = numberOfConsecutiveErrors;
		}

		public IEnumerable<object> RobustEnumeration(IEnumerator<object> input, IndexingFunc func)
		{
			return RobustEnumeration(input, new[] { func, });
		}

		public IEnumerable<object> RobustEnumeration(IEnumerator<object> input, IEnumerable<IndexingFunc> funcs)
		{
			var onlyIterateOverEnumableOnce = new List<object>();
			try
			{
				while (input.MoveNext())
				{
					onlyIterateOverEnumableOnce.Add(input.Current);
				}
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
							cancellationToken.ThrowIfCancellationRequested();
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
								// we explicitly do not dispose the enumerator, since that would not allow us 
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