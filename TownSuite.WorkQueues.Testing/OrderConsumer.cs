namespace TownSuite.WorkQueues.Testing
{
    public class OrderConsumer : IConsumer<OrderSubmitted>
    {
        public static int HandledCount { get; private set; } = 0;
        public Task Consume(ConsumeContext<OrderSubmitted> context)
        {
            Console.WriteLine($"Order received: {context.Message.ProductName}");
            HandledCount++;
            
            return Task.CompletedTask;
        }
    }
}
