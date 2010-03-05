using System;
using log4net;
using Raven.Database.Storage;
using Raven.Database.Tasks;
using Raven.Database.Extensions;

namespace Raven.Database.Indexing
{
    public class TaskExecuter
    {
        private readonly TransactionalStorage transactionalStorage;
        private readonly WorkContext context;
        private readonly ILog log = LogManager.GetLogger(typeof (TaskExecuter));

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
                    var taskAsJson = actions.GetFirstTask();
                    if (taskAsJson == null)
                    {
                        actions.Commit();
                        return;
                    }
                    log.DebugFormat("Executing {0}", taskAsJson);
                    foundWork = true;

                    ExecuteTask(taskAsJson);

                    actions.CompleteCurrentTask();
                    actions.Commit();
                });
                if(foundWork == false)
                    context.WaitForWork();
            }
        }

        private void ExecuteTask(string taskAsJson)
        {
            try
            {
                Task task = Task.ToTask(taskAsJson);
                try
                {
                    task.Execute(context);
                }
                catch (Exception e)
                {
                    log.WarnFormat(e, "Task {0} has failed and was deleted without completing any work", taskAsJson);
                }
            }
            catch (Exception e)
            {
                log.ErrorFormat(e, "Could not create instance of a task: {0}", taskAsJson);
            }
        }
    }
}