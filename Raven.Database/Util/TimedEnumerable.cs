using System;
using System.Collections;
using System.Collections.Generic;
using System.ComponentModel;
using System.Diagnostics;

namespace Raven.Database.Util
{
	public enum QueryTimings
	{
		[Description("Lucene search")]
		Lucene,

		[Description("Loading documents")]
		LoadDocuments,

		[Description("Transforming results")]
		TransformResults
	}

	public class TimedEnumerable<T> : IEnumerator<T>, IEnumerable<T>
	{
		private readonly IEnumerator<T> enumerator;

		private readonly Action<double> _finished;

		private readonly Stopwatch _watch;

		public TimedEnumerable(IEnumerable<T> enumerable, Action<double> finished)
		{
			enumerator = enumerable.GetEnumerator();
			_finished = finished;
			_watch = new Stopwatch();
		}

		public IEnumerator<T> GetEnumerator()
		{
			return this;
		}

		IEnumerator IEnumerable.GetEnumerator()
		{
			return GetEnumerator();
		}

		public void Dispose()
		{
			enumerator.Dispose();
		}

		public bool MoveNext()
		{
			_watch.Start();
			var moveNext = enumerator.MoveNext();
			Current = moveNext ? enumerator.Current : default(T);
			_watch.Stop();

			if (moveNext == false)
				_finished(_watch.Elapsed.TotalMilliseconds);

			return moveNext;
		}

		public void Reset()
		{
			throw new NotSupportedException();
		}

		public T Current { get; private set; }

		object IEnumerator.Current
		{
			get
			{
				return Current;
			}
		}
	}
}