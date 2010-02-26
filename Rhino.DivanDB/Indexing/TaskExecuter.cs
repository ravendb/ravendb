using System;
using Rhino.DivanDB.Storage;

namespace Rhino.DivanDB.Indexing
{
    public class TaskExecuter
    {
        private readonly TransactionalStorage transactionalStorage;
        private readonly WorkContext context;

        public TaskExecuter(TransactionalStorage transactionalStorage, WorkContext context)
        {
            this.transactionalStorage = transactionalStorage;
            this.context = context;
        }

        public void Execute()
        {
            while(context.DoWork)
            {
                bool foundWork = false;
                transactionalStorage.Batch(actions =>
                                           {
                                               var task = actions.GetTask();
                                               if(task == null)
                                               {
                                                   actions.Commit(); 
                                                   return;
                                               }
                                               foundWork = true;

                                               task.Execute(context);

                                               actions.CompleteCurrentTask();

                                               actions.Commit();
                                           });
                if(foundWork == false)
                    context.WaitForWork();
            }
        }
    }
}