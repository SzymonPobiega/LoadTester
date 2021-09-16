using System;
using System.Collections.Generic;
using System.Threading.Tasks;
using NServiceBus.Raw;
using Shared;
using Shared.Msmq;
using Shared.Sql;

namespace Processor
{
    class Program
    {
        static async Task Main(string[] commandLineArgs)
        {
            var transport = commandLineArgs[0];

            var connectionString = commandLineArgs.Length > 1 ? commandLineArgs[1] : null;

            var endpointName = commandLineArgs.Length > 2 ? commandLineArgs[2] : "Processor";

            var queueLengthProvider = Transports.Create(transport);
            queueLengthProvider.Initialize(connectionString, (entries, mapping) => { });

            var metrics = new Metrics();
            var reporter = new MetricsReporter(metrics, Console.WriteLine, TimeSpan.FromSeconds(2));
            reporter.Start();
            var counter = metrics.GetCounter("Messages received");

            var configuration = RawEndpointConfiguration.Create(endpointName, (context, dispatcher) =>
            {
                counter.Mark();
                return Task.CompletedTask;
            }, "poison");
            queueLengthProvider.ConfigureEndpoint(configuration);
            configuration.AutoCreateQueue();

            var endpoint = await RawEndpoint.Start(configuration);

            Console.WriteLine("Press <enter> to exit.");

            Console.ReadLine();

            await reporter.Stop();
            await endpoint.Stop();
        }
    }
}
