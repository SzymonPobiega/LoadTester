using System;
using System.Threading.Tasks;
using Microsoft.Data.SqlClient;
using NServiceBus;
using Shared;

namespace OutboxProcessor
{
    using NServiceBus.Pipeline;

    class Program
    {
        public static Counter MessagesReceived;

        static async Task Main(string[] commandLineArgs)
        {
            var connectionString = commandLineArgs[0];

            var config = new EndpointConfiguration("Processor");

            var persistence = config.UsePersistence<SqlPersistence>();
            persistence.ConnectionBuilder(() => new SqlConnection(connectionString));
            persistence.SqlDialect<SqlDialect.MsSqlServer>();
            persistence.SubscriptionSettings().DisableCache();
            var outbox = config.EnableOutbox();
            outbox.UsePessimisticConcurrencyControl();
            outbox.KeepDeduplicationDataFor(TimeSpan.FromMinutes(45));
            config.UseSerialization<NewtonsoftSerializer>();
            config.SendFailedMessagesTo("error");
            var transport = config.UseTransport<RabbitMQTransport>();
            transport.ConnectionString("host=localhost");
            transport.UseConventionalRoutingTopology();
            transport.Routing().RouteToEndpoint(typeof(AnotherMessage), "Sink");
            config.EnableInstallers();
            config.LimitMessageProcessingConcurrencyTo(32);
            config.Pipeline.Register(new DelayBehavior(), "Delay");

            var metrics = new Metrics();
            var reporter = new MetricsReporter(metrics, Console.WriteLine, TimeSpan.FromSeconds(2));
            reporter.Start();
            MessagesReceived = metrics.GetCounter("Messages received");

            var endpoint = await Endpoint.Start(config);

            Console.WriteLine("Press <enter> to exit.");

            Console.ReadLine();

            await reporter.Stop();
            await endpoint.Stop();
        }
    }

    class DelayBehavior : Behavior<IDispatchContext>
    {
        public override async Task Invoke(IDispatchContext context, Func<Task> next)
        {
            var random = new Random();
            await Task.Delay(random.Next(100));
            await next();
        }
    }

    class TestMessageHandler : IHandleMessages<TestMessage>
    {
        public async Task Handle(TestMessage message, IMessageHandlerContext context)
        {
            Program.MessagesReceived.Mark();
            var random = new Random();
            await Task.Delay(random.Next(100));
            await context.Send(new AnotherMessage
            {
                Data = message.Data
            });
        }
    }
}
