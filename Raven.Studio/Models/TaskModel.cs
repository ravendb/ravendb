using System.Windows.Input;
using Raven.Studio.Infrastructure;

namespace Raven.Studio.Models
{
    public abstract class TaskModel : NotifyPropertyChangedBase
    {
        public TaskModel()
        {
            Output = new BindableCollection<string>(new PrimaryKeyComparer<string>(x => x));
        }

        private string _name;
        public string Name
        {
            get { return _name; }
            set { _name = value; OnPropertyChanged(); }
        }

        public string Description { get; set; }

        public BindableCollection<string> Output { get; set; }

        public abstract ICommand Action { get; }
    }
}