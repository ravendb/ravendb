using System;
using System.Collections.Generic;
using System.Reflection;
using JetBrains.Annotations;
using Lucene.Net.Index;
using Lucene.Net.Search;
using Lucene.Net.Store;

namespace Raven.Server.Documents.Queries.Sorting.Custom
{
    public class TestFieldComparator : FieldComparator
    {
        private readonly FieldComparator _comparator;

        public readonly List<string> Debug;

        public TestFieldComparator([NotNull] FieldComparator comparator)
        {
            Debug = new List<string>();

            _comparator = comparator ?? throw new ArgumentNullException(nameof(comparator));

            TryAddDebugToComparator();
        }

        public override int Compare(int slot1, int slot2)
        {
            Executing(nameof(Compare), $"{nameof(slot1)} = {slot1}, {nameof(slot2)} = {slot2}");
            var result = _comparator.Compare(slot1, slot2);
            Executed(nameof(Compare), $"{nameof(slot1)} = {slot1}, {nameof(slot2)} = {slot2}", result);
            return result;
        }

        public override void SetBottom(int slot)
        {
            Executing(nameof(SetBottom), $"{nameof(slot)} = {slot}");
            _comparator.SetBottom(slot);
            Executed(nameof(SetBottom), $"{nameof(slot)} = {slot}");
        }

        public override int CompareBottom(int doc, IState state)
        {
            Executing(nameof(CompareBottom), $"{nameof(doc)} = {doc}");
            var result = _comparator.CompareBottom(doc, state);
            Executed(nameof(CompareBottom), $"{nameof(doc)} = {doc}", result);
            return result;
        }

        public override void Copy(int slot, int doc, IState state)
        {
            Executing(nameof(Copy), $"{nameof(slot)} = {slot}, {nameof(doc)} = {doc}");
            _comparator.Copy(slot, doc, state);
            Executed(nameof(Copy), $"{nameof(slot)} = {slot}, {nameof(doc)} = {doc}");
        }

        public override void SetNextReader(IndexReader reader, int docBase, IState state)
        {
            Executing(nameof(SetNextReader), $"{nameof(reader)} = {reader}, {nameof(docBase)} = {docBase}");
            _comparator.SetNextReader(reader, docBase, state);
            Executed(nameof(SetNextReader), $"{nameof(reader)} = {reader}, {nameof(docBase)} = {docBase}");
        }

        public override IComparable this[int slot] => _comparator[slot];

        private void Executed(string method, string args)
        {
            Debug.Add($"Executed '{method}' with '{args}' arguments.");
        }

        private void Executed(string method, string args, object result)
        {
            Debug.Add($"Executed '{method}' with '{args}' arguments. Result: {result}");
        }

        private void Executing(string method, string args)
        {
            Debug.Add($"Executing '{method}' with '{args}' arguments");
        }

        private void TryAddDebugToComparator()
        {
            Debug.Add("Looking for 'public List<string> Debug { get; set;} property in comparator...");

            var property = _comparator.GetType().GetProperty("Debug", BindingFlags.Instance | BindingFlags.Public);
            if (property == null)
            {
                Debug.Add("Couldn't find 'Debug' property. Skipping.");
                return;
            }

            try
            {
                property.SetValue(_comparator, Debug);

                Debug.Add("'Debug' property set.");
            }
            catch (Exception e)
            {
                Debug.Add($"Couldn't set 'Debug' property. Message: {e.Message}");
            }
        }
    }
}
