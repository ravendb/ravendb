using System;
using System.Web.Mvc;

namespace Raven.Client.MvcIntegration
{
    public class RecordCurrentControllerContextFilter: ActionFilterAttribute
    {
        [ThreadStatic] public static ControllerContext CurrentControllerContext;

        public override void OnActionExecuting(ActionExecutingContext filterContext)
        {
            CurrentControllerContext = filterContext.Controller.ControllerContext;
        }

        public override void OnActionExecuted(ActionExecutedContext filterContext)
        {
            CurrentControllerContext = null;
        }
    }
}
