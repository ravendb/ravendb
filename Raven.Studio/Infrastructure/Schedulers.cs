using System.Threading.Tasks;

namespace Raven.Studio.Infrastructure
{
    public static class Schedulers
    {
        public static TaskScheduler UIScheduler { get; set; }
    }
}