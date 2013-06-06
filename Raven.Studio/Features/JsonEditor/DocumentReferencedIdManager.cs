using System;
using System.Collections.Generic;
using System.Linq;
using System.Text.RegularExpressions;

namespace Raven.Studio.Features.JsonEditor
{
    public class DocumentReferencedIdManager
    {
        public event EventHandler<EventArgs> Changed;
        public event EventHandler<EventArgs> CurrentIdsChanged;

        private object gate = new object();
        private HashSet<string> knownIds = new HashSet<string>();
        private HashSet<string> knownInvalidIds = new HashSet<string>();
        private List<string> currentIds;

        protected void OnChanged(EventArgs e)
        {
            var handler = Changed;
            if (handler != null) handler(this, e);
        }

        protected virtual void OnCurrentIdsChanged()
        {
            var handler = CurrentIdsChanged;
            if (handler != null) handler(this, EventArgs.Empty);
        }

        public IList<String> CurrentIds
        {
            get
            {
                lock (gate)
                {
                    if (currentIds == null)
                    {
                        return new string[0];
                    }
                    else
                    {
                        return currentIds.AsReadOnly();
                    }
                }
            }
        } 

        public bool IsKnownInvalid(string value)
        {
            lock (gate)
            {
                return knownInvalidIds.Contains(value);
            }
        }

        public bool IsId(string value)
        {
            lock (gate)
            {
                return knownIds.Contains(value);
            }
        }

        public void Clear()
        {
            lock(gate)
            {
                knownIds.Clear();
                knownInvalidIds.Clear();
            }

            OnChanged(EventArgs.Empty);
        }

        public void AddKnownIds(IEnumerable<string> ids)
        {
            var any = false;
            lock(gate)
            {
                foreach (var id in ids)
                {
                    any = true;
                    knownIds.Add(id);
                }
            }

            if (any)
                OnChanged(EventArgs.Empty);
        }

        public void AddKnownInvalidIds(IEnumerable<string> ids)
        {
            lock (gate)
            {
                foreach (var id in ids)
                {
                    knownInvalidIds.Add(id);
                }
            }
        }

        public bool NeedsChecking(string id)
        {
            lock (gate)
            {
                return !knownInvalidIds.Contains(id) && !knownIds.Contains(id);
            }
        }

        public void UpdateCurrentIds(IEnumerable<string> potentialReferences)
        {
            lock (gate)
            {
                currentIds = potentialReferences.Where(id => knownIds.Contains(id)).ToList();
            }

            OnCurrentIdsChanged();
        }
    }
}