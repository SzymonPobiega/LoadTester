using System.Collections.Generic;
using Shared.Msmq;
using Shared.RabbitMq;
using Shared.Sql;

namespace Shared
{
    public static class Transports
    {
        static Dictionary<string, IProvideQueueLength> transports = new Dictionary<string, IProvideQueueLength>
        {
            {"sql", new SqlQueueLengthProvider()},
            {"msmq", new MsmqQueueLengthProvider()},
            {"rabbit", new RabbitMqQueueLengthProvider()}
        };

        public static IProvideQueueLength Create(string name)
        {
            return transports[name];
        }
    }
}