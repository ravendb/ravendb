using System;
using System.Net;
using System.Windows;
using System.Windows.Controls;
using System.Windows.Documents;
using System.Windows.Ink;
using System.Windows.Input;
using System.Windows.Media;
using System.Windows.Media.Animation;
using System.Windows.Shapes;

namespace VirtualCollection.VirtualCollection
{
    public class Disposer : IDisposable
    {
        private readonly Action _action;

        public Disposer(Action action)
        {
            _action = action;
        }

        public void Dispose()
        {
            _action();
        }
    }
}
