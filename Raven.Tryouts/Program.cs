using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Text;
using System.Threading;

namespace RavenRepro
{
    using System.Messaging;
    using System.Threading.Tasks;
    using System.Transactions;
    using Raven.Client;
    using Raven.Client.Document;

    class Program
    {
        const int NumTimeouts = 1000;
        static DocumentStore store;

        static Guid ResourceManagerId = new Guid("05216603-dd72-4ec5-88b6-0c88e7b74e05");

        static MessageQueue outputQueue;

        static string queueName = string.Format("FormatName:DIRECT=OS:localhost\\private$\\RavenRepro.Output");

        static void Main(string[] args)
        {
            store = new DocumentStore { Url = "http://localhost:8080", DefaultDatabase = "RavenRepro" };
            store.Initialize();






            //if(!MessageQueue.Exists(".\"))
            //    MessageQueue.Create(queueName, true);

            outputQueue = new MessageQueue(queueName, false, true, QueueAccessMode.Send);

	        Console.WriteLine("Ready");

            string cmd;

            while ((cmd = Console.ReadKey().Key.ToString().ToLower()) != "q")
            {
                switch (cmd)
                {
                    case "s":
                        SeedDB();
                        break;
                    case "v":
                        Verify();
                        break;
                    default:
                        DispatchTimeouts();
                        break;

                }
            }


        }

        static void Verify()
        {
            var numMessagesInQueue = new MessageQueue(queueName).GetAllMessages().Count();

            Console.Out.WriteLine("Messages in q: {0}", numMessagesInQueue);

        }

        static void DispatchTimeouts()
        {
            Console.Out.WriteLine("Getting existing timeouts");


            //get existing timeouts to dispatch
            IEnumerable<string> timeoutIds;
            do
            {

                using (var session = OpenSession())
                {
                    timeoutIds = session.Query<TimeoutData>().Select(t => t.Id).Take(1024).ToList();
                }


                Console.Out.WriteLine("Found {0} timeouts", timeoutIds.Count());
	            var random = new Random();
                //generate duplicates
	            for (int i = 0; i < 4; i++)
	            {
		            timeoutIds = timeoutIds.Concat(timeoutIds.OrderBy(x => random.Next()));
	            }


                Console.Out.WriteLine("Dispatching {0} timeouts", timeoutIds.Count());
				int count = 0;
				Parallel.ForEach(timeoutIds, (id) =>
				{
					if (Thread.VolatileRead(ref count) > (NumTimeouts / 3))
					{
						Console.Beep();
						Environment.FailFast("blah");
					}
					try
					{
						DispatchTimeout(id);
					}
					catch (Exception ex)
					{
						Console.Out.WriteLine("Dispatch failed {0}", ex.Message);
					}
					finally
					{
						Interlocked.Increment(ref count);
					}

				});
            } while (timeoutIds.Any());

            Console.Out.WriteLine("Done dispatching");

        }

        static void DispatchTimeout(string id)
        {
            using (var tx = new TransactionScope())
            {
                ForceDistributedTransaction();

                if (TryRemove(id))
                    Enqueue(id);

                tx.Complete();
            }
        }

        static void Enqueue(string id)
        {
            outputQueue.Send(new Message { Label = id }, id, MessageQueueTransactionType.Automatic);
        }

        static void SeedDB()
        {
            Console.Out.WriteLine("Clearing q");
            new MessageQueue(queueName).Purge();

            Console.Out.WriteLine("Seeding db");
            for (int i = 0; i < NumTimeouts; i++)
            {
                using (var session = store.OpenSession())
                {
                    session.Store(new TimeoutData
                        {
                            Dispatched = false
                        });

                    session.SaveChanges();
                }
            }

            Console.Out.WriteLine("Seeding done");

        }

        static bool TryRemove(string timeoutId)
        {
            using (var session = OpenSession())
            {
                var timeoutData = session.Load<TimeoutData>(timeoutId);

                if (timeoutData == null)
                    return false;

                timeoutData.Dispatched = true;
                session.SaveChanges();

                session.Delete(timeoutData);
                session.SaveChanges();

                return true;
            }
        }

        static IDocumentSession OpenSession()
        {
            var session = store.OpenSession();

            session.Advanced.AllowNonAuthoritativeInformation = false;
            session.Advanced.UseOptimisticConcurrency = true;

            return session;
        }

        static void ForceDistributedTransaction()
        {
            Transaction.Current.EnlistDurable(ResourceManagerId, new DummyEnlistmentNotification(),
                                              EnlistmentOptions.None);
        }
    }

    internal class TimeoutData
    {
        public string Id { get; set; }
        public bool Dispatched { get; set; }
    }

    public class DummyEnlistmentNotification : IEnlistmentNotification
    {
        public static readonly Guid Id = Guid.NewGuid();

        static Random rand = new Random();

        public bool WasCommitted { get; set; }

        public bool RandomRollback { get; set; }

        public void Prepare(PreparingEnlistment preparingEnlistment)
        {


            if (RandomRollback && rand.Next(0, 10) > 7)
                preparingEnlistment.ForceRollback();
            else
                preparingEnlistment.Prepared();
        }

        public void Commit(Enlistment enlistment)
        {
            WasCommitted = true;
            enlistment.Done();

        }

        public void Rollback(Enlistment enlistment)
        {
            enlistment.Done();
        }

        public void InDoubt(Enlistment enlistment)
        {
            enlistment.Done();
        }
    }
}
