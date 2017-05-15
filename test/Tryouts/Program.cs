using System;
using System.Linq;
using Raven.Client.Documents;

namespace Tryouts
{
    public class Program
    {
        public static void Main(string[] args)
        {
            var store = new DocumentStore
            {
                Url = "http://localhost.fiddler:8080",
                DefaultDatabase = "Tasks"
            };

            store.Initialize();

            //using (var session = store.OpenSession())
            //{
            //    var task = new ToDoTask
            //    {
            //        DueDate = DateTime.Today.AddDays(1),
            //        Task = "Buy milk"
            //    };
            //    session.Store(task);
            //    session.SaveChanges();
            //}

            //using (var session = store.OpenSession())
            //{
            //    var task = session.Load<ToDoTask>("ToDoTasks/1");
            //    task.Completed = true;
            //    session.SaveChanges();
            //}

            //using (var session = store.OpenSession())
            //{
            //    for (int i = 0; i < 5; i++)
            //    {
            //        session.Store(new ToDoTask
            //        {
            //            DueDate = DateTime.Today.AddDays(i),
            //            Task = "Take the dog for a walk"
            //        });
            //    }

            //    session.SaveChanges();
            //}


            using (var session = store.OpenSession())
            {
                var tasksPerDay =
                    from t in session.Query<ToDoTask>()
                    group t by t.DueDate
                    into g
                    select new
                    {
                        DueDate = g.Key,
                        TasksPerDate = g.Count()
                    };

                foreach (var taskSummary in tasksPerDay)
                {
                    Console.WriteLine($"{taskSummary.DueDate} - {taskSummary.TasksPerDate}");
                }
            }

        }
    }



    public class ToDoTask
    {
        public string Id { get; set; }
        public string Task { get; set; }
        public bool Completed { get; set; }
        public DateTime DueDate { get; set; }
    }
}