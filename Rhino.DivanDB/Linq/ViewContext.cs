using System;

namespace Rhino.DivanDB.Linq
{
    public class ViewContext
    {
        [ThreadStatic]
        public static string CurrentDocumentId;
    }
}