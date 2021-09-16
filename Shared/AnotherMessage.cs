using NServiceBus;

namespace Shared
{
    public class AnotherMessage : IMessage
    {
        public byte[] Data { get; set; }
    }
}