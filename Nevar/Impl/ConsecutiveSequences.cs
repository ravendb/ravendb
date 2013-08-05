using System.Collections;
using System.Collections.Generic;
using System.Linq;

namespace Nevar.Impl
{
	public class ConsecutiveSequences : IEnumerable<long>
	{
		private class Seq
		{
			public long Start;
			public int Count;
			public int AllocatingFor;

			public override string ToString()
			{
				return string.Format("Start: {0}, Count: {1}, AllocatingFor: {2}", Start, Count, AllocatingFor);
			}
		}

		public int Count { get; set; }

		private readonly Dictionary<long, Seq> _sequencesByLast = new Dictionary<long, Seq>();
		private readonly Dictionary<long, Seq> _sequencesByFirst = new Dictionary<long, Seq>();
		private readonly Dictionary<int, Seq> _sequencesByAllocationSize = new Dictionary<int, Seq>();

		public void Add(long v)
		{
			Count++;
			Seq seq;
			if (_sequencesByLast.TryGetValue(v - 1, out seq) == false)
			{
				if (_sequencesByFirst.TryGetValue(v + 1, out seq) == false)
				{
					var value = new Seq {Start = v, Count = 1};
					_sequencesByLast.Add(v, value);
					_sequencesByFirst.Add(v, value);
				}
				else
				{
					_sequencesByFirst.Remove(v + 1);
					seq.Count++;
					seq.Start = v;
					_sequencesByFirst[v] = seq;
				}
			}
			else
			{
				_sequencesByLast.Remove(v - 1);
				seq.Count++;
				_sequencesByLast[v] = seq;
			}
		}

		public bool TryAllocate(int num, out long v)
		{
			var allocationSize = NextPowerOfTwo(num);
			Seq seq;
			if (_sequencesByAllocationSize.TryGetValue(allocationSize, out seq))
			{
				var start = seq.Start;
				seq.Count -= num;
				seq.Start += num;
				_sequencesByFirst.Remove(start);
				if (seq.Count == 0)
				{
					_sequencesByLast.Remove(start);
				}
				else
				{
					_sequencesByFirst.Add(seq.Start, seq);
				}
				if (seq.Count < allocationSize) // can't serve the next request for this size
				{
					seq.AllocatingFor = 0;
					_sequencesByAllocationSize.Remove(allocationSize);
				}
				v = start;
				Count -= num;
				return true;
			}

			// let us try to find a sequence long enough that is suitable
			// we find the largest sequence that can serve, to make sure that we are serving from it
			// for as long as we can
			seq = _sequencesByLast.Values.Where(x => x.AllocatingFor == 0 && x.Count >= allocationSize)
			                .OrderByDescending(x => x.Count)
			                .FirstOrDefault();
			if (seq == null)
			{
				v = -1;
				return false;
			}

			seq.AllocatingFor = allocationSize;
			_sequencesByAllocationSize.Add(allocationSize, seq);
			return TryAllocate(num, out v);
		}

		//http://graphics.stanford.edu/~seander/bithacks.html#RoundUpPowerOf2
		int NextPowerOfTwo(int v)
		{
			v--;
			v |= v >> 1;
			v |= v >> 2;
			v |= v >> 4;
			v |= v >> 8;
			v |= v >> 16;
			v++;
			return v;
		}

		public IEnumerator<long> GetEnumerator()
		{
			foreach (var seq in _sequencesByFirst.Values)
			{
				for (int i = 0; i < seq.Count; i++)
				{
					yield return i + seq.Start;
				}
			}
		}

		IEnumerator IEnumerable.GetEnumerator()
		{
			return GetEnumerator();
		}
	}
}