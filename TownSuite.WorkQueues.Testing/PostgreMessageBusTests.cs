using Dapper;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.Logging;
using Npgsql;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using TownSuite.WorkQueues.Postgres;

namespace TownSuite.WorkQueues.Testing
{
    [TestFixture]
    public class PostgreMessageBusTests
    {
        [Test]
        public async Task PostgresqlMessageBusTest()
        {
            await using var wrapper = await TestContainerWrapper.CreateContainerAsync("postgres");
            await wrapper.StartAsync();
            
            var logger = Moq.Mock.Of<ILogger<PostgresMessageBus>>();
            var options = new SqlTransportOptions()
            {
                ConnectionString = wrapper.Container.GetConnectionString(),
                AdminConnectionString = wrapper.Container.GetConnectionString(),
                Schema = "public",   // matches what BringUpDatabasePostgresql creates
                AllowEmptyBatches = true,
                MaxBatchSize = 10,
                MaxWaitTime = TimeSpan.FromSeconds(1)
            };

            using (var bus = new PostgresMessageBus(options, logger))
            {
                // Register the consumer for the OrderSubmitted message.
                bus.Subscribe(new OrderConsumer());

                // Create and publish a range of order message.

                foreach (int i in Enumerable.Range(1, 10))
                {
                    var order = new OrderSubmitted
                    {
                        OrderId = Guid.NewGuid(),
                        ProductName = "Widget via PostgreSQL"
                    };
                    await bus.Publish(order);
                }

                await Task.Delay(2000); // Wait for processing.
                Console.WriteLine("Message published to PostgreSQL. Processing in the background...");
            }

            Assert.That(OrderConsumer.HandledCount, Is.EqualTo(10));
        }
        
    }
}