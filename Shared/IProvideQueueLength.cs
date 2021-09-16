using System;
using System.Threading.Tasks;
using NServiceBus.Raw;

namespace Shared
{
    public interface IProvideQueueLength
    {
        void ConfigureEndpoint(RawEndpointConfiguration configuration);
        void Initialize(string connectionString, Action<QueueLengthEntry[], EndpointToQueueMapping> store);
        void TrackEndpointInputQueue(EndpointToQueueMapping queueToTrack);
        Task Start();
        Task Stop();
    }
}