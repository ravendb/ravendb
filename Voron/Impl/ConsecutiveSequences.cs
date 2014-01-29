using System.Collections;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;

namespace Voron.Impl
{
	public class ConsecutiveSequences : IEnumerable<long>
	{
		public class Seq
		{
			public long Start;
			public int Count;

			public override string ToString()
			{
				return string.Format("Start: {0},Count: {1}", Start, Count);
			}
		}

		public int Count { get; set; }

		public int LargestSequence
		{
			get
			{
				for (int i = _sequencesBySize.Count - 1; i >= 0; i--)
				{
					var key = _sequencesBySize.Keys[i];
					if (_sequencesBySize[key].Count > 0)
						return key;
				}
				return 0;
			}
		}

		private readonly Dictionary<long, Seq> _sequencesByLast = new Dictionary<long, Seq>();
		private readonly Dictionary<long, Seq> _sequencesByFirst = new Dictionary<long, Seq>();
        private readonly SortedList<int, List<Seq>> _sequencesBySize = new SortedList<int, List<Seq>>();

		public void Add(long v)
		{
			Debug.Assert(_sequencesByFirst.ContainsKey(v) == false);
			Count++;
			Seq seq;
			if (_sequencesByLast.TryGetValue(v, out seq) == false)
			{
				if (_sequencesByFirst.TryGetValue(v + 1, out seq) == false)
				{
					seq = new Seq { Start = v, Count = 1 };
					_sequencesByLast.Add(v + 1, seq);
					_sequencesByFirst.Add(v, seq);
					AddToSetBySize(1, seq);
				}
				else
				{
					_sequencesByFirst.Remove(v + 1);
					_sequencesByLast.Remove(seq.Start + seq.Count);
					var prevCount = seq.Count;
					seq.Count++;
					seq.Start = v;
					Seq prev;
					while (_sequencesByLast.TryGetValue(seq.Start + seq.Count, out prev)) // merge backward
					{
						_sequencesByFirst.Remove(prev.Start);
						_sequencesByLast.Remove(prev.Start + prev.Count);
						RemoveFromSetBySize(prev.Count, prev);
						seq.Count += prev.Count;
						seq.Start = prev.Start;
					}
					_sequencesByFirst.Add(seq.Start, seq);
					_sequencesByLast.Add(seq.Start + seq.Count, seq);
					RemoveFromSetBySize(prevCount,seq);
					AddToSetBySize(seq.Count, seq);
				}
			}
			else
			{
				_sequencesByLast.Remove(v);
				var prevCount = seq.Count;
				seq.Count++;
				Seq next;
				while (_sequencesByFirst.TryGetValue(seq.Start + seq.Count, out next)) // merge forward
				{
					_sequencesByFirst.Remove(next.Start);
					_sequencesByLast.Remove(next.Start + next.Count);
					RemoveFromSetBySize(next.Count, next);
					seq.Count += next.Count;
				}
				_sequencesByLast.Add(seq.Start + seq.Count, seq);
				RemoveFromSetBySize(prevCount,seq);
				AddToSetBySize(seq.Count, seq);
			}
			Debug.Assert(_sequencesByFirst.Count == _sequencesByLast.Count);
		}

		private void RemoveFromSetBySize(int size, Seq seq)
		{
			List<Seq> set;
			if (_sequencesBySize.TryGetValue(size, out set) == false)
				return;
			set.Remove(seq);

			if (set.Count == 0)
				_sequencesBySize.Remove(size);
		}

		private void AddToSetBySize(int size, Seq seq)
		{
			List<Seq> list;
			if (_sequencesBySize.TryGetValue(size, out list) == false)
			{
                _sequencesBySize.Add(size, list = new List<Seq>());
			}
		    Debug.Assert(list.Exists(x => x.Start == seq.Start) == false);
			list.Add(seq);
		}

		public bool TryAllocate(int num, out long v)
		{
			if (_sequencesBySize.Count == 0)
			{
				v = -1;
				return false;
			}
            List<Seq> set;
			if (_sequencesBySize.TryGetValue(num, out set) && set.Count > 0)// this should catch a lot of size 1
			{
				v = AllocateFrom(num, set);
				return true;
			}
			if (_sequencesBySize.Keys[_sequencesBySize.Count - 1] < num) // there is no way we can find a match
			{
				v = -1;
				return false;
			}
			// can probably do binary search, but easier to just scan for now
			for (int i = 0; i < _sequencesBySize.Count; i++)
			{
				var key = _sequencesBySize.Keys[i];
				set = _sequencesBySize[key];
				if (key >= num && set.Count > 0)
				{
					v = AllocateFrom(num, set);
					return true;
				}
			}
			v = -1;
			return false;
		}

        private long AllocateFrom(int num, List<Seq> set)
        {
            var seq = set[0];
			RemoveFromSetBySize(seq.Count, seq);
			var start = seq.Start;
			var end = start + seq.Count;
			seq.Count -= num;
			seq.Start += num;
			_sequencesByFirst.Remove(start);
			if (seq.Count == 0)
			{
				_sequencesByLast.Remove(end);
			}
			else
			{
				_sequencesByFirst.Add(seq.Start, seq);
				AddToSetBySize(seq.Count, seq);
			}
			Count -= num;
			return start;
		}

		public IEnumerator<long> GetEnumerator()
		{
			foreach (var seq in _sequencesByFirst.Values)
			{
				for (int i = 0; i < seq.Count; i++)
				{
					yield return seq.Start + i;
				}
			}
		}

		IEnumerator IEnumerable.GetEnumerator()
		{
			return GetEnumerator();
		}
	}
}
