namespace TownSuite.WorkQueues.Testing
{
    public class OrderSubmitted
    {
        public Guid OrderId { get; set; }
        public string ProductName { get; set; }
    }
}
