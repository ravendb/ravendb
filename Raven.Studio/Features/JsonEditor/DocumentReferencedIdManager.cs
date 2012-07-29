using System;
using System.Collections.Generic;
using System.Net;
using System.Windows;
using System.Windows.Controls;
using System.Windows.Documents;
using System.Windows.Ink;
using System.Windows.Input;
using System.Windows.Media;
using System.Windows.Media.Animation;
using System.Windows.Shapes;

namespace Raven.Studio.Features.JsonEditor
{
    public class DocumentReferencedIdManager
    {
        public event EventHandler<EventArgs> Changed;
        private object gate = new object();
        private HashSet<string> knownIds = new HashSet<string>();
        private HashSet<string> knownInvalidIds = new HashSet<string>();
 
        protected void OnChanged(EventArgs e)
        {
            EventHandler<EventArgs> handler = Changed;
            if (handler != null) handler(this, e);
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
            {
                OnChanged(EventArgs.Empty);
            }
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
    }
}
