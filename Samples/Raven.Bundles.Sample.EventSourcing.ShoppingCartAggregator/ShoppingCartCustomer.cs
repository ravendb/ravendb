namespace Raven.Sample.EventSourcing
{
    public class ShoppingCartCustomer
    {
        public string Id { get; set; }
        public string Name { get; set; }

        public override string ToString()
        {
            return string.Format("Id: {0}, Name: {1}", Id, Name);
        }
    }
}