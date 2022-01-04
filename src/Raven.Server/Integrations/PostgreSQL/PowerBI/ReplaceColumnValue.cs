namespace Raven.Server.Integrations.PostgreSQL.PowerBI
{
    public class ReplaceColumnValue
    {
        public string DstColumnName { get; set; }

        public string SrcColumnName { get; set; }

        public object OldValue { get; set; }

        public object NewValue { get; set; }
    }
}
