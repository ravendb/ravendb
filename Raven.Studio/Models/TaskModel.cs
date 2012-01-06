using System.Windows.Input;
using Raven.Studio.Infrastructure;

namespace Raven.Studio.Models
{
    public abstract class TaskModel : NotifyPropertyChangedBase
    {
        public TaskModel()
        {
            Output = new BindableCollection<string>(x => x);
        }

        private string name;
        public string Name
        {
            get { return name; }
            set { name = value; OnPropertyChanged(); }
        }

        public string Description { get; set; }

        public BindableCollection<string> Output { get; set; }

        public abstract ICommand Action { get; }
    }
}