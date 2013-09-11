using System;
using System.Net;
using System.Threading.Tasks;
using System.Windows;
using System.Windows.Controls;
using System.Windows.Documents;
using System.Windows.Ink;
using System.Windows.Input;
using System.Windows.Media;
using System.Windows.Media.Animation;
using System.Windows.Shapes;

namespace Raven.Studio.Infrastructure
{
    public class AsyncActionCommand : Command
    {
        private readonly Func<object, Task> execute;

        public AsyncActionCommand(Func<object, Task> execute)
        {
            this.execute = execute;
        }

        public AsyncActionCommand(Func<Task> execute)
        {
            this.execute = _ => execute();
        }

        protected override System.Threading.Tasks.Task ExecuteAsync(object parameter)
        {
            return execute(parameter);
        }
    }
}
