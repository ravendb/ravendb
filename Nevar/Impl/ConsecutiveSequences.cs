using System.Collections;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;

namespace Nevar.Impl
{
    public class ConsecutiveSequences : IEnumerable<long>
    {
        private readonly int _minSequence;

        public class Seq
        {
            public long Start;
            public int Count;

            public override string ToString()
            {
                return string.Format("Start: {0},Count: {1}",Start,Count);
            }
        }

        public ConsecutiveSequences(int minSequence = 1)
        {
            _minSequence = minSequence;
        }

        public int Count { get; set; }

        private readonly Dictionary<long,Seq> _sequencesByLast = new Dictionary<long,Seq>();
        private readonly Dictionary<long,Seq> _sequencesByFirst = new Dictionary<long,Seq>();
        private Seq _current;

        /// <summary>
        /// This assumes that the caller code will not try to send overlapping sequences
        /// </summary>
        public void Load(long start,int count)
        {
            var seq = new Seq { Start = start,Count = count };
            _sequencesByFirst[start] = seq;
            _sequencesByLast[start + count] = seq;
            Count += count;
        }

        public void Add(long v)
        {
            Debug.Assert(_sequencesByFirst.ContainsKey(v) == false);
            Count++;
            Seq seq;
            if (_sequencesByLast.TryGetValue(v,out seq) == false)
            {
                if (_sequencesByFirst.TryGetValue(v + 1,out seq) == false)
                {
                    var value = new Seq { Start = v,Count = 1 };
                    _sequencesByLast.Add(v + 1,value);
                    _sequencesByFirst.Add(v,value);
                }
                else
                {
                    _sequencesByFirst.Remove(v + 1);
                    _sequencesByLast.Remove(seq.Start + seq.Count);
                    seq.Count++;
                    seq.Start = v;
                    Seq prev;
                    while (_sequencesByLast.TryGetValue(seq.Start + seq.Count,out prev)) // merge backward
                    {
                        _sequencesByFirst.Remove(prev.Start);
                        _sequencesByLast.Remove(prev.Start + prev.Count);

                        seq.Count += prev.Count;
                        seq.Start = prev.Start;
                    }
                    _sequencesByFirst.Add(seq.Start,seq);
                    _sequencesByLast.Add(seq.Start + seq.Count,seq);
                }
            }
            else
            {
                _sequencesByLast.Remove(v);
                seq.Count++;
                Seq next;
                while (_sequencesByFirst.TryGetValue(seq.Start + seq.Count,out next)) // merge forward
                {
                    _sequencesByFirst.Remove(next.Start);
                    _sequencesByLast.Remove(next.Start + next.Count);

                    seq.Count += next.Count;
                }
                _sequencesByLast.Add(seq.Start + seq.Count,seq);
            }
            Debug.Assert(_sequencesByFirst.Count == _sequencesByLast.Count);
        }

        public bool TryAllocate(int num,out long v)
        {
            if (_current != null && _current.Count >= num)
            {
                var start = _current.Start;
                var end = start + _current.Count;
                _current.Count -= num;
                _current.Start += num;
                _sequencesByFirst.Remove(start);
                if (_current.Count == 0)
                {
                    _sequencesByLast.Remove(end);
                }
                else
                {
                    _sequencesByFirst.Add(_current.Start,_current);
                }
                v = start;
                Count -= num;
                if (_current.Count == 0)
                {
                    _current = null;
                }
                return true;
            }

            // let us try to find a sequence long enough that is suitable
            // we find the largest sequence that can serve,to make sure that we are serving from it
            // for as long as we can
            _current = _sequencesByLast.Values.Where(x => x.Count >= num && x.Count >= _minSequence)
                            .OrderByDescending(x => x.Count)
                            .FirstOrDefault();
            if (_current == null)
            {
                v = -1;
                return false;
            }

            return TryAllocate(num,out v);
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
